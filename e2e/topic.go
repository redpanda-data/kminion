package e2e

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// Check our end-to-end test topic and adapt accordingly if something does not match our expectations.
// - does it exist?
//
// - is it configured correctly?
//   - does it have enough partitions?
//   - is the replicationFactor correct?
//
// - are assignments good?
//   - is each broker leading at least one partition?
//   - are replicas distributed correctly?
func (s *Service) validateManagementTopic(ctx context.Context) error {
	s.logger.Debug("validating end-to-end topic...")

	meta, err := s.getTopicMetadata(ctx)
	if err != nil {
		return fmt.Errorf("validateManagementTopic cannot get metadata of e2e topic: %w", err)
	}

	typedErr := kerr.TypedErrorForCode(meta.Topics[0].ErrorCode)
	topicExists := false
	switch {
	case typedErr == nil:
		topicExists = true
	case errors.Is(typedErr, kerr.UnknownTopicOrPartition):
		// UnknownTopicOrPartition (Error code 3) means that the topic does not exist.
		// When the topic doesn't exist, continue to create it further down in the code.
		topicExists = false
	default:
		// If the topic (possibly) exists, but there's an error, then this should result in a fail
		return fmt.Errorf("failed to get metadata for end-to-end topic: %w", err)
	}

	// Create topic if it doesn't exist
	if !topicExists {
		if !s.config.TopicManagement.Enabled {
			return fmt.Errorf("the configured end to end topic does not exist. The topic will not be created " +
				"because topic management is disabled")
		}

		if err = s.createManagementTopic(ctx, meta); err != nil {
			return err
		}

		// Topic was just created with optimal assignments from the partition planner.
		// We can skip the validation/planning phase since the topic already has the correct
		// partition count and optimal replica assignments. We only need to update our
		// internal partition count tracking for KMinion's e2e monitoring operations.
		return s.updatePartitionCount(ctx)
	}

	// Topic already exists - use partition planner to validate and potentially fix assignments
	planner := NewPartitionPlanner(s.config.TopicManagement, s.logger)
	plan, err := planner.Plan(meta)
	if err != nil {
		return fmt.Errorf("failed to create partition plan: %w", err)
	}

	// Convert the plan to Kafka requests
	topicName := pointerStrToStr(meta.Topics[0].Topic)
	alterReq, createReq := plan.ToRequests(topicName)

	// Log detailed operations only if there are changes planned
	if len(plan.Reassignments) > 0 || len(plan.CreateAssignments) > 0 {
		s.logPlannedOperations(meta, plan, topicName)
	}

	err = s.executeAlterPartitionAssignments(ctx, alterReq)
	if err != nil {
		return fmt.Errorf("failed to alter partition assignments: %w", err)
	}

	err = s.executeCreatePartitions(ctx, createReq)
	if err != nil {
		return fmt.Errorf("failed to create partitions: %w", err)
	}

	return s.updatePartitionCount(ctx)
}

// updatePartitionCount retrieves metadata to inform kminion about the updated
// partition count of its e2e topic. It must be updated after topic validation
// because the validation process may lead to the creation of new partitions.
// This can occur when new brokers are added to the cluster.
func (s *Service) updatePartitionCount(ctx context.Context) error {
	retryTicker := time.NewTicker(1 * time.Second)
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retryTicker.C:
			meta, err := s.getTopicMetadata(ctx)
			if err != nil {
				return fmt.Errorf("could not get topic metadata while updating partition count: %w", err)
			}

			typedErr := kerr.TypedErrorForCode(meta.Topics[0].ErrorCode)
			if typedErr == nil {
				s.partitionCount = len(meta.Topics[0].Partitions)
				s.logger.Debug("updatePartitionCount: successfully updated partition count", zap.Int("partition_count", s.partitionCount))
				return nil
			}
			if !errors.Is(typedErr, kerr.UnknownTopicOrPartition) {
				return fmt.Errorf("unexpected error while updating partition count: %w", typedErr)
			}
			s.logger.Warn("updatePartitionCount: received UNKNOWN_TOPIC_OR_PARTITION error, possibly due to timing issue. Retrying...")
			// The UNKNOWN_TOPIC_OR_PARTITION error occurs occasionally even though the topic is created
			// in the validateManagementTopic function. It appears to be a timing issue where the topic metadata
			// is not immediately available after creation. In practice, waiting for a short period and then retrying
			// the operation resolves the issue.
		}
	}
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
		err := kerr.ErrorForCode(topic.ErrorCode)
		if err != nil {
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
		s.logger.Error("alter partition assignments: failed to alter partition assignments", zap.Any("request_topics", req.Topics))
		return fmt.Errorf("inner Kafka error: %w", typedErr)
	}
	for _, topic := range res.Topics {
		for _, partition := range topic.Partitions {
			typedErr = kerr.TypedErrorForCode(partition.ErrorCode)
			if typedErr != nil {
				return fmt.Errorf("inner Kafka partition error on partition '%v': %w", partition.Partition, typedErr)
			}
		}
	}

	return nil
}

// logPlannedOperations logs detailed information about current state and planned changes
func (s *Service) logPlannedOperations(meta *kmsg.MetadataResponse, plan *Plan, topicName string) {
	topicMeta := meta.Topics[0]

	// Log current partition state
	s.logger.Info("current partition assignments for e2e topic",
		zap.String("topic", topicName),
		zap.Int("current_partitions", len(topicMeta.Partitions)),
		zap.Int("brokers_available", len(meta.Brokers)),
	)

	// Log each current partition assignment (sorted by partition ID)
	sortedPartitions := make([]kmsg.MetadataResponseTopicPartition, len(topicMeta.Partitions))
	copy(sortedPartitions, topicMeta.Partitions)
	sort.Slice(sortedPartitions, func(i, j int) bool {
		return sortedPartitions[i].Partition < sortedPartitions[j].Partition
	})

	for _, partition := range sortedPartitions {
		s.logger.Info("current partition assignment",
			zap.String("topic", topicName),
			zap.Int32("partition", partition.Partition),
			zap.Int32s("replicas", partition.Replicas),
			zap.Int32("leader", partition.Leader),
		)
	}

	// Log reassignment operations
	if len(plan.Reassignments) > 0 {
		s.logger.Info("planned partition reassignments",
			zap.String("topic", topicName),
			zap.Int("reassignment_count", len(plan.Reassignments)),
		)

		// Sort reassignments by partition ID for consistent logging
		sortedReassignments := make([]Reassignment, len(plan.Reassignments))
		copy(sortedReassignments, plan.Reassignments)
		sort.Slice(sortedReassignments, func(i, j int) bool {
			return sortedReassignments[i].Partition < sortedReassignments[j].Partition
		})

		for _, reassignment := range sortedReassignments {
			// Find current assignment for this partition
			var currentReplicas []int32
			var currentLeader int32 = -1
			for _, partition := range topicMeta.Partitions {
				if partition.Partition == reassignment.Partition {
					currentReplicas = partition.Replicas
					currentLeader = partition.Leader
					break
				}
			}

			s.logger.Info("partition reassignment",
				zap.String("topic", topicName),
				zap.Int32("partition", reassignment.Partition),
				zap.Int32s("current_replicas", currentReplicas),
				zap.Int32s("new_replicas", reassignment.Replicas),
				zap.Int32("current_leader", currentLeader),
				zap.Int32("new_leader", reassignment.Replicas[0]),
			)
		}
	}

	// Log creation operations
	if len(plan.CreateAssignments) > 0 {
		s.logger.Info("planned partition creations",
			zap.String("topic", topicName),
			zap.Int("creation_count", len(plan.CreateAssignments)),
			zap.Int("current_partitions", len(topicMeta.Partitions)),
			zap.Int("final_partitions", plan.FinalPartitionCount),
		)

		nextPartitionID := int32(len(topicMeta.Partitions))
		for i, creation := range plan.CreateAssignments {
			s.logger.Info("new partition creation",
				zap.String("topic", topicName),
				zap.Int32("new_partition", nextPartitionID+int32(i)),
				zap.Int32s("replicas", creation.Replicas),
				zap.Int32("leader", creation.Replicas[0]),
			)
		}
	}

	// Log final expected state summary
	s.logger.Info("final expected state summary",
		zap.String("topic", topicName),
		zap.Int("total_partitions", plan.FinalPartitionCount),
		zap.Int("reassignments_applied", len(plan.Reassignments)),
		zap.Int("new_partitions_created", len(plan.CreateAssignments)),
	)
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

	// Use partition planner to determine optimal assignments for the new topic.
	// The metadata already contains broker info, and since the topic doesn't exist,
	// meta.Topics[0].Partitions will be empty, which is exactly what we want.
	planner := NewPartitionPlanner(topicCfg, s.logger)
	plan, err := planner.Plan(allMeta)
	if err != nil {
		return fmt.Errorf("failed to create partition plan for new topic: %w", err)
	}

	// Create topic with specific replica assignments from the planner
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = topicCfg.Name
	topic.NumPartitions = -1     // Must be -1 when using ReplicaAssignment
	topic.ReplicationFactor = -1 // Must be -1 when using ReplicaAssignment
	topic.Configs = createTopicConfig(topicCfg)

	// Convert planner's CreateAssignments to Kafka's ReplicaAssignment format
	for i, assignment := range plan.CreateAssignments {
		replica := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		replica.Partition = int32(i)
		replica.Replicas = append([]int32(nil), assignment.Replicas...)
		topic.ReplicaAssignment = append(topic.ReplicaAssignment, replica)
	}

	req := kmsg.NewCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return fmt.Errorf("failed to create e2e topic: %w", err)
	}
	if len(res.Topics) > 0 {
		err := kerr.ErrorForCode(res.Topics[0].ErrorCode)
		if err != nil {
			return fmt.Errorf("failed to create e2e topic: %w", err)
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

//nolint:unused
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
	// real persistence beyond a few minutes; it might be good to keep messages
	// around a bit for debugging.
	return []kmsg.CreateTopicsRequestTopicConfig{
		topicConfig("cleanup.policy", "delete"),
		topicConfig("segment.ms", (time.Hour * 12).Milliseconds()),   // new segment every 12h
		topicConfig("retention.ms", (time.Hour * 24).Milliseconds()), // discard segments older than 24h
		topicConfig("min.insync.replicas", minISR),
	}
}
