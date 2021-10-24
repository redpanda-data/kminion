package e2e

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// Check our end-to-end test topic and adapt accordingly if something does not match our expectations.
// - does it exist?
// - is it configured correctly?
//     - does it have enough partitions?
//     - is the replicationFactor correct?
// - are assignments good?
//     - is each broker leading at least one partition?
//     - are replicas distributed correctly?
func (s *Service) validateManagementTopic(ctx context.Context) error {
	s.logger.Debug("validating end-to-end topic...")

	meta, err := s.getTopicMetadata(ctx)
	if err != nil {
		return fmt.Errorf("validateManagementTopic cannot get metadata of e2e topic: %w", err)
	}

	// Create topic if it doesn't exist
	topicExists := len(meta.Topics) == 1
	if !topicExists {
		if !s.config.TopicManagement.Enabled {
			return fmt.Errorf("the configured end to end topic does not exist. The topic will not be created " +
				"because topic management is disabled")
		}

		if err = s.createManagementTopic(ctx, meta); err != nil {
			return err
		}
	}

	alterReq, createReq, err := s.calculatePartitionReassignments(meta)
	if err != nil {
		return fmt.Errorf("failed to calculate partition reassignments: %w", err)
	}

	err = s.executeAlterPartitionAssignments(ctx, alterReq)
	if err != nil {
		return fmt.Errorf("failed to alter partition assignments: %w", err)
	}

	err = s.executeCreatePartitions(ctx, createReq)
	if err != nil {
		return fmt.Errorf("failed to create partitions: %w", err)
	}

	return nil
}

func (s *Service) executeCreatePartitions(ctx context.Context, req *kmsg.CreatePartitionsRequest) error {
	if req == nil {
		return nil
	}

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return err
	}

	for _, topic := range res.Topics {
		typedErr := kerr.TypedErrorForCode(topic.ErrorCode)
		if typedErr != nil {
			return fmt.Errorf("inner Kafka error: %w", err)
		}
	}

	return nil
}

func (s *Service) executeAlterPartitionAssignments(ctx context.Context, req *kmsg.AlterPartitionAssignmentsRequest) error {
	if req == nil {
		return nil
	}

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return err
	}

	typedErr := kerr.TypedErrorForCode(res.ErrorCode)
	if typedErr != nil {
		return fmt.Errorf("inner Kafka error: %w", err)
	}
	for _, topic := range res.Topics {
		for _, partition := range topic.Partitions {
			err = kerr.TypedErrorForCode(partition.ErrorCode)
			if err != nil {
				return fmt.Errorf("inner Kafka partition error on partition '%v': %w", partition.Partition, err)
			}
		}
	}

	return nil
}

func (s *Service) calculatePartitionReassignments(meta *kmsg.MetadataResponse) (*kmsg.AlterPartitionAssignmentsRequest, *kmsg.CreatePartitionsRequest, error) {
	brokerByID := brokerMetadataByBrokerID(meta.Brokers)
	topicMeta := meta.Topics[0]
	desiredReplicationFactor := s.config.TopicManagement.ReplicationFactor

	if desiredReplicationFactor > len(brokerByID) {
		return nil, nil, fmt.Errorf("the desired replication factor of '%v' is larger than the available brokers "+
			"('%v' brokers)", desiredReplicationFactor, len(brokerByID))
	}

	// We want to ensure that each brokerID leads at least one partition permanently. Hence let's iterate over brokers.
	preferredLeaderPartitionsBrokerID := make(map[int32][]kmsg.MetadataResponseTopicPartition)
	for _, broker := range brokerByID {
		for _, partition := range topicMeta.Partitions {
			// PreferredLeader = BrokerID of the brokerID that is the desired leader. Regardless who the current leader is
			preferredLeader := partition.Replicas[0]
			if broker.NodeID == preferredLeader {
				preferredLeaderPartitionsBrokerID[broker.NodeID] = append(preferredLeaderPartitionsBrokerID[broker.NodeID], partition)
			}
		}
	}

	// Partitions that use the same brokerID more than once as preferred leader can be reassigned to other brokers
	// We collect them to avoid creating new partitions when not needed.
	reassignablePartitions := make([]kmsg.MetadataResponseTopicPartition, 0)
	for _, partitions := range preferredLeaderPartitionsBrokerID {
		if len(partitions) > 1 {
			reassignablePartitions = append(reassignablePartitions, partitions[1:]...)
			continue
		}
	}

	// Now let's try to reassign (or create) new partitions for those brokers that are not the preferred leader for
	// any partition.

	partitionCount := len(topicMeta.Partitions)
	partitionReassignments := make([]kmsg.AlterPartitionAssignmentsRequestTopicPartition, 0)
	createPartitionAssignments := make([]kmsg.CreatePartitionsRequestTopicAssignment, 0)

	for brokerID, partitions := range preferredLeaderPartitionsBrokerID {
		// Add replicas if number of replicas is smaller than desiredReplicationFactor
		for _, partition := range partitions {
			if len(partition.Replicas) < desiredReplicationFactor {
				req := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
				req.Partition = partition.Partition
				req.Replicas = s.calculateAppropriateReplicas(meta, desiredReplicationFactor, brokerByID[brokerID])
				partitionReassignments = append(partitionReassignments, req)
			}
		}

		// TODO: Consider more than one partition per broker config
		if len(partitions) != 0 {
			continue
		}

		// Let's try to use one of the existing partitions before we decide to create new partitions
		if len(reassignablePartitions) > 0 {
			partition := reassignablePartitions[0]
			req := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
			req.Partition = partition.Partition
			req.Replicas = s.calculateAppropriateReplicas(meta, desiredReplicationFactor, brokerByID[brokerID])
			partitionReassignments = append(partitionReassignments, req)

			reassignablePartitions = reassignablePartitions[1:]
		}

		// Create a new partition for this broker
		partitionCount++
		assignmentReq := kmsg.NewCreatePartitionsRequestTopicAssignment()
		assignmentReq.Replicas = s.calculateAppropriateReplicas(meta, desiredReplicationFactor, brokerByID[brokerID])
		createPartitionAssignments = append(createPartitionAssignments, assignmentReq)
	}

	var reassignmentReq *kmsg.AlterPartitionAssignmentsRequest
	if len(partitionReassignments) > 0 {
		s.logger.Info("e2e probe topic has to be modified due to missing replicas or wrong preferred leader assignments",
			zap.Int("partition_count", len(topicMeta.Partitions)),
			zap.Int("broker_count", len(meta.Brokers)),
			zap.Int("config_partitions_per_broker", s.config.TopicManagement.PartitionsPerBroker),
			zap.Int("config_replication_factor", s.config.TopicManagement.ReplicationFactor),
			zap.Int("partitions_to_reassign", len(partitionReassignments)),
		)

		r := kmsg.NewAlterPartitionAssignmentsRequest()
		reassignmentTopicReq := kmsg.NewAlterPartitionAssignmentsRequestTopic()
		reassignmentTopicReq.Partitions = partitionReassignments
		reassignmentTopicReq.Topic = *topicMeta.Topic
		r.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{reassignmentTopicReq}
		reassignmentReq = &r
	}

	var createReq *kmsg.CreatePartitionsRequest
	if len(createPartitionAssignments) > 0 {
		s.logger.Info("e2e probe topic does not have enough partitions. Will add partitions to the topic...",
			zap.Int("actual_partition_count", len(topicMeta.Partitions)),
			zap.Int("broker_count", len(meta.Brokers)),
			zap.Int("config_partitions_per_broker", s.config.TopicManagement.PartitionsPerBroker),
			zap.Int("partitions_to_add", len(createPartitionAssignments)),
		)
		r := kmsg.NewCreatePartitionsRequest()
		createPartitionsTopicReq := kmsg.NewCreatePartitionsRequestTopic()
		createPartitionsTopicReq.Topic = s.config.TopicManagement.Name
		createPartitionsTopicReq.Assignment = createPartitionAssignments
		createPartitionsTopicReq.Count = int32(partitionCount)
		r.Topics = []kmsg.CreatePartitionsRequestTopic{createPartitionsTopicReq}
		createReq = &r
	}

	return reassignmentReq, createReq, nil
}

// calculateAppropriateReplicas returns the best possible brokerIDs that shall be used as replicas.
// It takes care of the brokers' rack awareness and general distribution among the available brokers.
func (s *Service) calculateAppropriateReplicas(meta *kmsg.MetadataResponse, replicationFactor int, leaderBroker kmsg.MetadataResponseBroker) []int32 {
	brokersWithoutLeader := make([]kmsg.MetadataResponseBroker, 0, len(meta.Brokers)-1)
	for _, broker := range meta.Brokers {
		if broker.NodeID == leaderBroker.NodeID {
			continue
		}
		brokersWithoutLeader = append(brokersWithoutLeader, broker)
	}
	brokersByRack := brokerMetadataByRackID(brokersWithoutLeader)

	replicasPerRack := make(map[string]int)
	replicas := make([]int32, replicationFactor)
	replicas[0] = leaderBroker.NodeID

	for rack := range brokersByRack {
		replicasPerRack[rack] = 0
	}
	replicasPerRack[pointerStrToStr(leaderBroker.Rack)]++

	popBrokerFromRack := func(rackID string) kmsg.MetadataResponseBroker {
		broker := brokersByRack[rackID][0]
		if len(brokersByRack[rackID]) == 1 {
			delete(brokersByRack, rackID)
		} else {
			brokersByRack[rackID] = brokersByRack[rackID][1:]
		}
		return broker
	}

	for i := 1; i < len(replicas); i++ {
		// Find best rack
		min := math.MaxInt32
		bestRack := ""
		for rack, replicaCount := range replicasPerRack {
			if replicaCount < min {
				bestRack = rack
				min = replicaCount
			}
		}

		replicas[i] = popBrokerFromRack(bestRack).NodeID
		replicasPerRack[bestRack]++
	}

	return replicas
}

func (s *Service) createManagementTopic(ctx context.Context, allMeta *kmsg.MetadataResponse) error {
	topicCfg := s.config.TopicManagement
	brokerCount := len(allMeta.Brokers)
	totalPartitions := brokerCount * topicCfg.PartitionsPerBroker

	s.logger.Info("e2e topic does not exist, creating it...",
		zap.String("topic_name", topicCfg.Name),
		zap.Int("partitions_per_broker", topicCfg.PartitionsPerBroker),
		zap.Int("replication_factor", topicCfg.ReplicationFactor),
		zap.Int("broker_count", brokerCount),
		zap.Int("total_partitions", totalPartitions),
	)

	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = topicCfg.Name
	topic.NumPartitions = int32(totalPartitions)
	topic.ReplicationFactor = int16(topicCfg.ReplicationFactor)
	topic.Configs = createTopicConfig(topicCfg)

	req := kmsg.NewCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return fmt.Errorf("failed to create e2e topic: %w", err)
	}
	if len(res.Topics) > 0 {
		if res.Topics[0].ErrorMessage != nil && *res.Topics[0].ErrorMessage != "" {
			return fmt.Errorf("failed to create e2e topic: %s", *res.Topics[0].ErrorMessage)
		}
	}

	return nil
}

func (s *Service) getTopicMetadata(ctx context.Context) (*kmsg.MetadataResponse, error) {
	topicReq := kmsg.NewMetadataRequestTopic()
	topicName := s.config.TopicManagement.Name
	topicReq.Topic = &topicName

	req := kmsg.NewMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{topicReq}

	return req.RequestWith(ctx, s.client)
}

func (s *Service) getTopicsConfigs(ctx context.Context, configNames []string) (*kmsg.DescribeConfigsResponse, error) {
	req := kmsg.NewDescribeConfigsRequest()
	req.IncludeDocumentation = false
	req.IncludeSynonyms = false
	req.Resources = []kmsg.DescribeConfigsRequestResource{
		{
			ResourceType: kmsg.ConfigResourceTypeTopic,
			ResourceName: s.config.TopicManagement.Name,
			ConfigNames:  configNames,
		},
	}

	return req.RequestWith(ctx, s.client)
}

func createTopicConfig(cfgTopic EndToEndTopicConfig) []kmsg.CreateTopicsRequestTopicConfig {

	topicConfig := func(name string, value interface{}) kmsg.CreateTopicsRequestTopicConfig {
		prop := kmsg.NewCreateTopicsRequestTopicConfig()
		prop.Name = name
		valStr := fmt.Sprintf("%v", value)
		prop.Value = &valStr
		return prop
	}

	minISR := 1
	if cfgTopic.ReplicationFactor >= 3 {
		// Only with 3+ replicas does it make sense to require acks from 2 brokers
		// todo: think about if we should change how 'producer.requiredAcks' works.
		//       we probably don't even need this configured on the topic directly...
		minISR = 2
	}

	// Even though kminion's end-to-end feature actually does not require any
	// real persistence beyond a few minutes; it might be good too keep messages
	// around a bit for debugging.
	return []kmsg.CreateTopicsRequestTopicConfig{
		topicConfig("cleanup.policy", "delete"),
		topicConfig("segment.ms", (time.Hour * 12).Milliseconds()),   // new segment every 12h
		topicConfig("retention.ms", (time.Hour * 24).Milliseconds()), // discard segments older than 24h
		topicConfig("min.insync.replicas", minISR),
	}
}
