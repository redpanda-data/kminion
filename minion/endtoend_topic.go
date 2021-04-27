package minion

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) validateManagementTopic(ctx context.Context) error {

	expectedReplicationFactor := s.Cfg.EndToEnd.TopicManagement.ReplicationFactor
	expectedNumPartitionsPerBroker := s.Cfg.EndToEnd.TopicManagement.PartitionsPerBroker
	topicMetadata, err := s.getTopicMetadata(ctx)
	if err != nil {
		return err
	}

	// If metadata is not reachable, then there is a problem in connecting to broker or lack of Authorization
	// TopicMetadataArray could be empty, therefore needs to do this check beforehand
	topicMetadataArray := topicMetadata.Topics
	if len(topicMetadataArray) == 0 {
		return fmt.Errorf("Unable to retrieve metadata, please make sure the brokers are up and/or you have right to access them")
	}
	doesTopicReachable := topicMetadata.Topics[0].Topic != ""
	if !doesTopicReachable {
		return fmt.Errorf("Unable to retrieve metadata, please make sure the brokers are up and/or you have right to access them")
	}

	// Create the management end to end topic if it does not exist
	doesTopicExist := topicMetadata.Topics[0].Partitions != nil
	if !doesTopicExist {
		err = s.createManagementTopic(ctx, topicMetadata)
		if err != nil {
			return err
		}
		return nil
	}

	// If the number of broker is less than expected Replication Factor it means the cluster brokers number is too small
	// topicMetadata.Brokers will return all the available brokers from the cluster
	isNumBrokerValid := len(topicMetadata.Brokers) >= expectedReplicationFactor
	if !isNumBrokerValid {
		return fmt.Errorf("Current cluster size differs from the expected size. Expected broker: %v NumOfBroker: %v", len(topicMetadata.Brokers), expectedReplicationFactor)
	}

	// Check the number of Partition per broker, if it is too low create partition
	// topicMetadata.Topics[0].Partitions is the number of PartitionsPerBroker
	isTotalPartitionTooLow := len(topicMetadata.Topics[0].Partitions) < expectedNumPartitionsPerBroker
	if isTotalPartitionTooLow {
		// Create partition if the number partition is lower, can't delete partition
		assignment := kmsg.NewCreatePartitionsRequestTopicAssignment()
		assignment.Replicas = topicMetadata.Topics[0].Partitions[0].Replicas

		topic := kmsg.NewCreatePartitionsRequestTopic()
		topic.Topic = s.Cfg.EndToEnd.TopicManagement.Name
		topic.Count = int32(expectedNumPartitionsPerBroker) // Should be greater than current partition number
		topic.Assignment = []kmsg.CreatePartitionsRequestTopicAssignment{assignment}

		create := kmsg.NewCreatePartitionsRequest()
		create.Topics = []kmsg.CreatePartitionsRequestTopic{topic}
		_, err := create.RequestWith(ctx, s.kafkaSvc.Client)
		if err != nil {
			return fmt.Errorf("failed to do kmsg request on creating partitions: %w", err)
		}
		return nil
	}

	// Check distinct Leader Nodes, if it is more than replicationFactor it means the partitions got assigned wrongly
	distinctLeaderNodes := []int32{}
	for _, partition := range topicMetadata.Topics[0].Partitions {
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
	assignmentInvalid := len(distinctLeaderNodes) != s.Cfg.EndToEnd.TopicManagement.ReplicationFactor
	// Reassign Partitions on invalid assignment
	if assignmentInvalid {
		// Get the new AssignedReplicas by checking the ReplicationFactor config
		assignedReplicas := make([]int32, s.Cfg.EndToEnd.TopicManagement.ReplicationFactor)
		for index := range assignedReplicas {
			assignedReplicas[index] = int32(index)
		}

		// Generate the partition assignments from PartitionPerBroker config
		partitions := make([]int32, s.Cfg.EndToEnd.TopicManagement.PartitionsPerBroker)
		reassignedPartitions := []kmsg.AlterPartitionAssignmentsRequestTopicPartition{}
		for index := range partitions {
			rp := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
			rp.Partition = int32(index)
			rp.Replicas = assignedReplicas
			reassignedPartitions = append(reassignedPartitions, rp)
		}

		managamentTopicReassignment := kmsg.NewAlterPartitionAssignmentsRequestTopic()
		managamentTopicReassignment.Topic = s.Cfg.EndToEnd.TopicManagement.Name
		managamentTopicReassignment.Partitions = reassignedPartitions

		reassignment := kmsg.NewAlterPartitionAssignmentsRequest()
		reassignment.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{managamentTopicReassignment}

		_, err := reassignment.RequestWith(ctx, s.kafkaSvc.Client)
		if err != nil {
			return fmt.Errorf("failed to do kmsg request on topic reassignment: %w", err)
		}
		return nil
	}

	return nil
}

func getTopicConfig(cfgTopic EndToEndTopicConfig) []kmsg.CreateTopicsRequestTopicConfig {

	minISRConf := kmsg.NewCreateTopicsRequestTopicConfig()
	minISR := strconv.Itoa(cfgTopic.ReplicationFactor)
	minISRConf.Name = "min.insync.replicas"
	minISRConf.Value = &minISR

	cleanupPolicyConf := kmsg.NewCreateTopicsRequestTopicConfig()
	cleanupStr := "delete"
	cleanupPolicyConf.Name = "cleanup.policy"
	cleanupPolicyConf.Value = &cleanupStr

	retentionByteConf := kmsg.NewCreateTopicsRequestTopicConfig()
	retentionStr := "10000000"
	retentionByteConf.Name = "retention.bytes"
	retentionByteConf.Value = &retentionStr

	segmentByteConf := kmsg.NewCreateTopicsRequestTopicConfig()
	segmentStr := "1000000"
	segmentByteConf.Name = "segment.bytes"
	segmentByteConf.Value = &segmentStr

	return []kmsg.CreateTopicsRequestTopicConfig{
		minISRConf,
		cleanupPolicyConf,
		retentionByteConf,
		segmentByteConf,
	}
}

func (s *Service) createManagementTopic(ctx context.Context, topicMetadata *kmsg.MetadataResponse) error {

	s.logger.Info(fmt.Sprintf("creating topic %s for EndToEnd metrics", s.Cfg.EndToEnd.TopicManagement.Name))

	cfgTopic := s.Cfg.EndToEnd.TopicManagement
	topicConfigs := getTopicConfig(cfgTopic)

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

	res, err := req.RequestWith(ctx, s.kafkaSvc.Client)
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

	cfg := s.Cfg.EndToEnd.TopicManagement
	topicReq := kmsg.NewMetadataRequestTopic()
	topicReq.Topic = &cfg.Name

	req := kmsg.NewMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{topicReq}

	res, err := req.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to request metadata: %w", err)
	}

	return res, nil
}

func (s *Service) initEndToEnd(ctx context.Context) {

	reconciliationInterval := s.Cfg.EndToEnd.TopicManagement.ReconciliationInterval
	c1 := make(chan error, 1)

	// Run long running function on validating or reconciling that might be timeout
	go func() {
		err := s.validateManagementTopic(ctx)
		c1 <- err
	}()

	// Listen on our channel AND a timeout channel - which ever happens first.
	select {
	case err := <-c1:
		s.logger.Warn("failed to validate management topic for endtoend metrics", zap.Error(err))
		return
	case <-time.After(reconciliationInterval):
		s.logger.Warn("time exceeded while validating/reconciling management topic of endtoend metrics")
		return
	default:
		go s.ConsumeFromManagementTopic(ctx)

		t := time.NewTicker(s.Cfg.EndToEnd.ProbeInterval)
		for range t.C {
			s.ProduceToManagementTopic(ctx)
		}
	}
}

func timeNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
