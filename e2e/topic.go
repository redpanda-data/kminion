package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// Check our end-to-end test topic
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
	if len(meta.Topics) == 0 {
		if err = s.createManagementTopic(ctx, meta); err != nil {
			return err
		}
	}

	// Ensure the topic has enough partitions
	if err = s.ensureEnoughPartitions(ctx, meta); err != nil {
		return err
	}

	// Validate assignments
	if err = s.validatePartitionAssignments(ctx, meta); err != nil {
		return err
	}

	return nil
}

func (s *Service) validatePartitionAssignments(ctx context.Context, meta *kmsg.MetadataResponse) error {
	// We use a very simple strategy to distribute all partitions and its replicas to the brokers
	//
	// For example if we had:
	//   - 5 brokers
	//   - 5 partitions (partitionsPerBroker = 1)
	//   - replicationFactor of 5
	// then our assignments would look like the table below.
	// Why does this example use 5 partitions?
	//   Because for end-to-end testing to make sense, we'd like to report the message roundtrip latency for each broker.
	//   That's why we ensure that we always have at least as many partitions as there are brokers, so every broker
	//   can be the leader of at least one partition.
	//
	// The numbers after each partition are the brokerIds (0, 1, 2, 3, 4)
	// The first broker in an assignment array is the leader for that partition,
	// the following broker ids are hosting the partitions' replicas.
	//
	// Partition 0: [0, 1, 2, 3, 4]
	// Partition 1: [1, 2, 3, 4, 0]
	// Partition 2: [2, 3, 4, 0, 1]
	// Partition 3: [3, 4, 0, 1, 2]
	// Partition 4: [4, 0, 1, 2, 3]
	//
	// In addition to being very simple, this also has the benefit that each partitionID neatly corresponds
	// its leaders brokerID - at least most of the time.
	// When a broker suddenly goes offline, or a new one is added to the cluster, etc the assignments
	// might be off for a short period of time, but the assignments will be fixed automatically
	// in the next, periodic, topic validation (configured by 'topicManagement.reconciliationInterval')
	//

	topicName := s.config.TopicManagement.Name
	topicMeta := meta.Topics[0]
	realPartitionCount := len(topicMeta.Partitions)
	realReplicationFactor := len(topicMeta.Partitions[0].Replicas)

	// 1. Calculate the expected assignments
	allPartitionAssignments := make([][]int32, realPartitionCount)
	for i := range allPartitionAssignments {
		allPartitionAssignments[i] = make([]int32, realReplicationFactor)
	}

	// simple helper function that just keeps returning values from 0 to brokerCount forever.
	brokerIterator := func(start int32) func() int32 {
		brokerIndex := start
		return func() int32 {
			result := brokerIndex                                      // save result we'll return now
			brokerIndex = (brokerIndex + 1) % int32(len(meta.Brokers)) // prepare for next call: add one, and wrap around
			return result                                              // return current result
		}
	}

	startBroker := brokerIterator(0)
	for _, partitionAssignments := range allPartitionAssignments {
		// determine the leader broker: first partition will get 0, next one will get 1, ...
		start := startBroker()
		// and create an iterator that goes over all brokers, starting at our leader
		nextReplica := brokerIterator(start)

		for i := range partitionAssignments {
			partitionAssignments[i] = nextReplica()
		}
	}

	// 2. Check
	// Now that every partition knows which brokers are hosting its replicas, and which broker is the leader (i.e. is hosting the primary "replica"),
	// we just have to check if the current/real assignments are equal to our desired assignments (and then apply them if not).
	assignmentsAreEqual := true
	for partitionId := 0; partitionId < realPartitionCount; partitionId++ {
		expectedAssignments := allPartitionAssignments[partitionId]
		actualAssignments := topicMeta.Partitions[partitionId].Replicas

		// Check if any replica is assigned to the wrong broker
		for i := 0; i < realReplicationFactor; i++ {
			expectedBroker := expectedAssignments[i]
			actualBroker := actualAssignments[i]
			if expectedBroker != actualBroker {
				assignmentsAreEqual = false
			}
		}
	}
	if assignmentsAreEqual {
		// assignments are already exactly as they are supposed to be
		return nil
	}

	// 3. Apply
	// Some partitions have their replicas hosted on the wrong brokers!
	// Apply our desired replica configuration
	partitionReassignments := make([]kmsg.AlterPartitionAssignmentsRequestTopicPartition, realPartitionCount)
	for i := range partitionReassignments {
		partitionReassignments[i] = kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
		partitionReassignments[i].Partition = int32(i)
		partitionReassignments[i].Replicas = allPartitionAssignments[i]
	}

	topicReassignment := kmsg.NewAlterPartitionAssignmentsRequestTopic()
	topicReassignment.Topic = topicName
	topicReassignment.Partitions = partitionReassignments

	reassignRq := kmsg.NewAlterPartitionAssignmentsRequest()
	reassignRq.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{topicReassignment}

	reassignRes, err := reassignRq.RequestWith(ctx, s.client)
	if err != nil {
		// error while sending
		return fmt.Errorf("topic reassignment request failed: %w", err)
	}
	reassignErr := kerr.ErrorForCode(reassignRes.ErrorCode)
	if reassignErr != nil || (reassignRes.ErrorMessage != nil && *reassignRes.ErrorMessage != "") {
		// global error
		return fmt.Errorf(fmt.Sprintf("topic reassignment failed with ErrorMessage=\"%v\": %v",
			*reassignRes.ErrorMessage,
			safeUnwrap(reassignErr),
		))
	}

	// errors for individual partitions
	for _, t := range reassignRes.Topics {
		for _, p := range t.Partitions {
			pErr := kerr.ErrorForCode(p.ErrorCode)
			if pErr != nil || (p.ErrorMessage != nil && *p.ErrorMessage != "") {
				return fmt.Errorf(fmt.Sprintf("topic reassignment failed on partition %v with ErrorMessage=\"%v\": %v",
					p.Partition,
					safeUnwrap(pErr),
					*p.ErrorMessage),
				)
			}
		}
	}

	return nil
}

func (s *Service) ensureEnoughPartitions(ctx context.Context, meta *kmsg.MetadataResponse) error {
	partitionsPerBroker := s.config.TopicManagement.PartitionsPerBroker
	expectedPartitions := partitionsPerBroker * len(meta.Brokers)

	if len(meta.Topics[0].Partitions) >= expectedPartitions {
		return nil // no need to add more
	}

	partitionsToAdd := expectedPartitions - len(meta.Topics[0].Partitions)
	s.logger.Warn("e2e test topic does not have enough partitions, partitionCount is less than brokerCount * partitionsPerBroker. will add partitions to the topic...",
		zap.Int("expectedPartitionCount", expectedPartitions),
		zap.Int("actualPartitionCount", len(meta.Topics[0].Partitions)),
		zap.Int("brokerCount", len(meta.Brokers)),
		zap.Int("config.partitionsPerBroker", s.config.TopicManagement.PartitionsPerBroker),
		zap.Int("partitionsToAdd", partitionsToAdd),
	)

	topic := kmsg.NewCreatePartitionsRequestTopic()
	topic.Topic = s.config.TopicManagement.Name
	topic.Count = int32(expectedPartitions)

	// For each partition we're about to add, we need to define its replicas
	for i := 0; i < partitionsToAdd; i++ {
		assignment := kmsg.NewCreatePartitionsRequestTopicAssignment()
		// In order to keep the code as simple as possible, just copy the assignments from the first partition.
		// After the topic is created, there is another validation step that will take care of bad assignments!
		assignment.Replicas = meta.Topics[0].Partitions[0].Replicas
		topic.Assignment = append(topic.Assignment, assignment)
	}

	// Send request
	create := kmsg.NewCreatePartitionsRequest()
	create.Topics = []kmsg.CreatePartitionsRequestTopic{topic}
	createPartitionsResponse, err := create.RequestWith(ctx, s.client)

	// Check for errors
	if err != nil {
		return fmt.Errorf("request to create more partitions for e2e topic failed: %w", err)
	}
	nestedErrors := 0
	for _, topicResponse := range createPartitionsResponse.Topics {
		tErr := kerr.ErrorForCode(topicResponse.ErrorCode)
		if tErr != nil || (topicResponse.ErrorMessage != nil && *topicResponse.ErrorMessage != "") {
			s.logger.Error("error in createPartitionsResponse",
				zap.String("topic", topicResponse.Topic),
				zap.Stringp("errorMessage", topicResponse.ErrorMessage),
				zap.NamedError("topicError", tErr),
			)
			nestedErrors++
		}
	}
	if nestedErrors > 0 {
		return fmt.Errorf("request to add more partitions to e2e topic had some nested errors, see the %v log lines above", nestedErrors)
	}

	return nil
}

func (s *Service) createManagementTopic(ctx context.Context, allMeta *kmsg.MetadataResponse) error {
	topicCfg := s.config.TopicManagement
	brokerCount := len(allMeta.Brokers)
	totalPartitions := brokerCount * topicCfg.PartitionsPerBroker

	s.logger.Info("e2e topic does not exist, creating it...",
		zap.String("topicName", topicCfg.Name),
		zap.Int("partitionsPerBroker", topicCfg.PartitionsPerBroker),
		zap.Int("replicationFactor", topicCfg.ReplicationFactor),
		zap.Int("brokerCount", brokerCount),
		zap.Int("totalPartitions", totalPartitions),
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
