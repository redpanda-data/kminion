package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func (s *Service) ConsumeFromManagementTopic(ctx context.Context) error {
	client := s.client
	topicName := s.config.TopicManagement.Name
	topic := kgo.ConsumeTopics(kgo.NewOffset().AtEnd(), topicName)
	balancer := kgo.Balancers(kgo.CooperativeStickyBalancer()) // Default GroupBalancer
	switch s.config.Consumer.RebalancingProtocol {
	case RoundRobin:
		balancer = kgo.Balancers(kgo.RoundRobinBalancer())
	case Range:
		balancer = kgo.Balancers(kgo.RangeBalancer())
	case Sticky:
		balancer = kgo.Balancers(kgo.StickyBalancer())
	}
	client.AssignPartitions(topic)

	// Create a consumer group with the prefix
	groupId := fmt.Sprintf("%v-%v", s.config.Consumer.GroupIdPrefix, s.minionID)
	client.AssignGroup(groupId, kgo.GroupTopics(topicName), balancer, kgo.DisableAutoCommit())
	s.logger.Info("Starting to consume end-to-end", zap.String("topicName", topicName), zap.String("groupId", groupId))

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := client.PollFetches(ctx)
			errors := fetches.Errors()
			for _, err := range errors {
				// Log all errors and continue afterwards as we might get errors and still have some fetch results
				s.logger.Error("kafka fetch error",
					zap.String("topic", err.Topic),
					zap.Int32("partition", err.Partition),
					zap.Error(err.Err))
			}

			receiveTimestampMs := timeNowMs()

			//
			// Process messages
			iter := fetches.RecordIter()
			var record *kgo.Record
			for !iter.Done() {
				record = iter.Next()

				if record == nil {
					continue
				}

				s.processMessage(record, receiveTimestampMs)
			}

			//
			// Commit offsets for processed messages
			// todo: the normal way to commit offsets with franz-go is pretty good, but in our special case
			// 		 we want to do it manually, seperately for each partition, so we can track how long it took

			// todo: use findGroupCoordinatorID
			// maybe ask travis about return value, we want to know what coordinator the offsets was committed to
			// kminion probably already exposed coordinator for every group

			// if uncommittedOffset := client.UncommittedOffsets(); uncommittedOffset != nil {

			// 	startCommitTimestamp := timeNowMs()

			// 	client.CommitOffsets(ctx, uncommittedOffset, func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
			// 		// got commit response
			// 		if err != nil {
			// 			s.logger.Error(fmt.Sprintf("record had an error on commit: %v\n", err))
			// 			return
			// 		}

			// 		latencyMs := timeNowMs() - startCommitTimestamp
			// 		commitLatency := time.Duration(latencyMs * float64(time.Millisecond))

			// 		s.onOffsetCommit(commitLatency)
			// 	})
			// }
		}
	}

}

// todo: then also create a "tracker" that knows about in-flight messages, and the latest successful roundtrips

// processMessage takes a message and:
// - checks if it matches minionID and latency
// - updates metrics accordingly
func (s *Service) processMessage(record *kgo.Record, receiveTimestampMs float64) {
	var msg EndToEndMessage
	if jerr := json.Unmarshal(record.Value, &msg); jerr != nil {
		return // maybe older version
	}

	if msg.MinionID != s.minionID {
		return // not from us
	}

	latency := time.Duration((receiveTimestampMs - msg.Timestamp) * float64(time.Millisecond))

	s.onRoundtrip(record.Partition, latency)
}
