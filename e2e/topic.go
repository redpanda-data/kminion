package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) validateManagementTopic(ctx context.Context) error {

	s.logger.Info("validating end-to-end topic...")

	// expectedReplicationFactor := s.config.TopicManagement.ReplicationFactor
	expectedPartitionsPerBroker := s.config.TopicManagement.PartitionsPerBroker

	// Check how many brokers we have
	allMeta, err := s.getAllTopicsMetadata(ctx)
	if err != nil {
		return fmt.Errorf("validateManagementTopic cannot get metadata of all brokers/topics: %w", err)
	}
	expectedPartitions := expectedPartitionsPerBroker * len(allMeta.Brokers)

	topicMeta, err := s.getTopicMetadata(ctx)
	if err != nil {
		return err
	}

	// If metadata is not reachable, then there is a problem in connecting to broker or lack of Authorization
	// TopicMetadataArray could be empty, therefore needs to do this check beforehand

	// todo: check if len(meta.Topics) != 1
	// todo: assign topic := meta.Topics[0]
	// if len(meta.Topics) != 1 {
	// return fmt.Errorf("topic metadata request returned != 1 topics, please make sure the brokers are up and/or you have right to access them. got %v topics but expected 1", len(topicMetadataArray))
	// }

	// Create the management end to end topic if it does not exist
	topicExists := topicMeta.Topics[0].Partitions != nil
	if !topicExists {
		s.logger.Warn("end-to-end testing topic does not exist, will create it...")
		err = s.createManagementTopic(ctx, topicMeta)
		if err != nil {
			return err
		}
		return nil
	}

	// If the number of broker is less than expected Replication Factor it means the cluster brokers number is too small
	// topicMetadata.Brokers will return all the available brokers from the cluster

	// todo:
	// isNumBrokerValid := len(topicMeta.Brokers) >= expectedReplicationFactor
	// if !isNumBrokerValid {
	// 	return fmt.Errorf("current cluster size differs from the expected size (based on config topicManagement.replicationFactor). expected broker: %v NumOfBroker: %v", len(topicMeta.Brokers), expectedReplicationFactor)
	// }

	// Check the number of Partitions per broker, if it is too low create partition
	// topicMetadata.Topics[0].Partitions is the number of PartitionsPerBroker
	if len(topicMeta.Topics[0].Partitions) < expectedPartitions {
		s.logger.Warn("e2e test topic does not have enough partitions, partitionCount is less than brokerCount * partitionsPerBroker. will add partitions to the topic...",
			zap.Int("expectedPartitionCount", expectedPartitions),
			zap.Int("actualPartitionCount", len(topicMeta.Topics[0].Partitions)),
			zap.Int("brokerCount", len(allMeta.Brokers)),
			zap.Int("config.partitionsPerBroker", s.config.TopicManagement.PartitionsPerBroker),
		)
		// Create partition if the number partition is lower, can't delete partition
		assignment := kmsg.NewCreatePartitionsRequestTopicAssignment()
		assignment.Replicas = topicMeta.Topics[0].Partitions[0].Replicas

		topic := kmsg.NewCreatePartitionsRequestTopic()
		topic.Topic = s.config.TopicManagement.Name

		topic.Count = int32(expectedPartitionsPerBroker) // Should be greater than current partition number
		topic.Assignment = []kmsg.CreatePartitionsRequestTopicAssignment{assignment}

		create := kmsg.NewCreatePartitionsRequest()
		create.Topics = []kmsg.CreatePartitionsRequestTopic{topic}
		_, err := create.RequestWith(ctx, s.client)
		if err != nil {
			return fmt.Errorf("failed to add partitions to topic: %w", err)
		}

		// todo: why return? shouldn't we check for distinct leaders anyway??
		return nil
	}

	// Check distinct Leader Nodes, if it is more than replicationFactor it means the partitions got assigned wrongly
	// todo: rewrite this. its entirely possible that one broker leads multiple partitions (for example when a broker was temporarily offline).
	//       we only have to ensure that every available broker leads at least one of our partitions.

	distinctLeaderNodes := []int32{}
	for _, partition := range topicMeta.Topics[0].Partitions {
		if len(distinctLeaderNodes) == 0 {
			distinctLeaderNodes = append(distinctLeaderNodes, partition.Leader)
		} else {
			// Only append on distinct
			distinct := true
			for _, leaderNode := range distinctLeaderNodes {
				if partition.Leader == leaderNode {
					distinct = false
				}
			}
			if distinct {
				distinctLeaderNodes = append(distinctLeaderNodes, partition.Leader)
			}
		}
	}
	assignmentInvalid := len(distinctLeaderNodes) != s.config.TopicManagement.ReplicationFactor
	// Reassign Partitions on invalid assignment
	if assignmentInvalid {

		s.logger.Warn("e2e test topic partition assignments are invalid. not every broker leads at least one partition. will reassign partitions...",
			zap.String("todo", "not yet implemented"),
		)
		return nil

		// Get the new AssignedReplicas by checking the ReplicationFactor config
		assignedReplicas := make([]int32, s.config.TopicManagement.ReplicationFactor)
		for index := range assignedReplicas {
			assignedReplicas[index] = int32(index)
		}

		// Generate the partition assignments from PartitionPerBroker config
		partitions := make([]int32, s.config.TopicManagement.PartitionsPerBroker)
		reassignedPartitions := []kmsg.AlterPartitionAssignmentsRequestTopicPartition{}
		for index := range partitions {
			rp := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
			rp.Partition = int32(index)
			rp.Replicas = assignedReplicas
			reassignedPartitions = append(reassignedPartitions, rp)
		}

		managamentTopicReassignment := kmsg.NewAlterPartitionAssignmentsRequestTopic()
		managamentTopicReassignment.Topic = s.config.TopicManagement.Name
		managamentTopicReassignment.Partitions = reassignedPartitions

		reassignment := kmsg.NewAlterPartitionAssignmentsRequest()
		reassignment.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{managamentTopicReassignment}

		_, err := reassignment.RequestWith(ctx, s.client)
		if err != nil {
			return fmt.Errorf("failed to do kmsg request on topic reassignment: %w", err)
		}
		return nil
	}

	return nil
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

func (s *Service) createManagementTopic(ctx context.Context, topicMetadata *kmsg.MetadataResponse) error {

	s.logger.Info(fmt.Sprintf("creating topic %s for EndToEnd metrics", s.config.TopicManagement.Name))

	cfgTopic := s.config.TopicManagement
	topicConfigs := createTopicConfig(cfgTopic)

	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = cfgTopic.Name
	topic.NumPartitions = int32(cfgTopic.PartitionsPerBroker)
	topic.ReplicationFactor = int16(cfgTopic.ReplicationFactor)
	topic.Configs = topicConfigs

	// Workaround for wrong assignment on 1 ReplicationFactor with automatic assignment on topic creation, this will create the assignment manually, automatic assignment works on more than 1 RepFactor
	// Issue: Instead of putting the number of PartitionPerBroker in One Broker/Replica, the client will assign one partition on different Broker/Replica
	// Example for 1 RepFactor and 2 PartitionPerBroker: Instead of 2 Partitions on 1 Broker, it will put 1 Partition each on 2 Brokers
	if cfgTopic.ReplicationFactor == 1 {
		brokerID := topicMetadata.Brokers[0].NodeID
		var assignment []kmsg.CreateTopicsRequestTopicReplicaAssignment
		partitions := make([]int32, cfgTopic.PartitionsPerBroker)
		for index := range partitions {
			replicaAssignment := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
			replicaAssignment.Partition = int32(index)
			replicaAssignment.Replicas = []int32{brokerID}
			assignment = append(assignment, replicaAssignment)
		}
		topic.NumPartitions = -1     // Need to set this as -1 on Manual Assignment
		topic.ReplicationFactor = -1 // Need to set this as -1 on Manual Assignment
		topic.ReplicaAssignment = assignment
	}

	req := kmsg.NewCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}

	res, err := req.RequestWith(ctx, s.client)
	// Sometimes it won't throw Error, but the Error will be abstracted to res.Topics[0].ErrorMessage
	if res.Topics[0].ErrorMessage != nil {
		return fmt.Errorf("failed to create topic: %s", *res.Topics[0].ErrorMessage)
	}

	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
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
func (s *Service) getAllTopicsMetadata(ctx context.Context) (*kmsg.MetadataResponse, error) {

	// need to request metadata of all topics in order to get metadata of all brokers
	// not sure if there is a better way (copied from kowl)
	req := kmsg.NewMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{} // empty array should get us all topics, in all kafka versions

	return req.RequestWith(ctx, s.client)
}

func (s *Service) initEndToEnd(ctx context.Context) {

	validateTopicTicker := time.NewTicker(s.config.TopicManagement.ReconciliationInterval)
	produceTicker := time.NewTicker(s.config.ProbeInterval)
	commitTicker := time.NewTicker(5 * time.Second)
	// stop tickers when context is cancelled
	go func() {
		<-ctx.Done()
		produceTicker.Stop()
		validateTopicTicker.Stop()
		commitTicker.Stop()
	}()

	// keep checking end-to-end topic
	go func() {
		for range validateTopicTicker.C {
			err := s.validateManagementTopic(ctx)
			if err != nil {
				s.logger.Error("failed to validate end-to-end topic", zap.Error(err))
			}
		}
	}()

	// keep track of groups, delete old unused groups
	go s.groupTracker.start()

	// start consuming topic
	go s.startConsumeMessages(ctx)

	// start comitting offsets
	go func() {
		for range commitTicker.C {
			s.commitOffsets(ctx)
		}
	}()

	// start producing to topic
	go func() {
		for range produceTicker.C {
			s.produceLatencyMessages(ctx)
		}
	}()

}
