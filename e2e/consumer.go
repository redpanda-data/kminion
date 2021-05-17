package e2e

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

	// Create our own consumer group
	client.AssignGroup(s.groupId, kgo.GroupTopics(topicName), balancer, kgo.DisableAutoCommit())
	s.logger.Info("Starting to consume end-to-end", zap.String("topicName", topicName), zap.String("groupId", s.groupId))

	// Keep checking for the coordinator
	var currentCoordinator atomic.Value
	currentCoordinator.Store(kgo.BrokerMetadata{})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				describeReq := kmsg.NewDescribeGroupsRequest()
				describeReq.Groups = []string{s.groupId}
				describeReq.IncludeAuthorizedOperations = false

				shards := client.RequestSharded(ctx, &describeReq)
				for _, shard := range shards {
					// since we're only interested in the coordinator, we only check for broker errors on the response that contains our group
					response, ok := shard.Resp.(*kmsg.DescribeGroupsResponse)
					if !ok {
						s.logger.Warn("cannot cast shard response to DescribeGroupsResponse")
						continue
					}
					if len(response.Groups) == 0 {
						s.logger.Warn("DescribeGroupsResponse contained no groups")
						continue
					}
					group := response.Groups[0]
					groupErr := kerr.ErrorForCode(group.ErrorCode)
					if groupErr != nil {
						s.logger.Error("couldn't describe end-to-end consumer group, error in group", zap.Error(groupErr), zap.Any("broker", shard.Meta))
						continue
					}

					currentCoordinator.Store(shard.Meta)
					break
				}
			}
		}
	}()

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

			if uncommittedOffset := client.UncommittedOffsets(); uncommittedOffset != nil {

				startCommitTimestamp := timeNowMs()

				client.CommitOffsets(ctx, uncommittedOffset, func(_ *kmsg.OffsetCommitRequest, r *kmsg.OffsetCommitResponse, err error) {
					// got commit response
					latencyMs := timeNowMs() - startCommitTimestamp
					commitLatency := time.Duration(latencyMs * float64(time.Millisecond))

					if err != nil {
						s.logger.Error("offset commit failed", zap.Error(err), zap.Int64("latencyMilliseconds", commitLatency.Milliseconds()))
						return
					}

					// todo: check each partitions error code

					// only report commit latency if the coordinator is known
					coordinator := currentCoordinator.Load().(kgo.BrokerMetadata)
					if len(coordinator.Host) > 0 {
						s.onOffsetCommit(commitLatency, coordinator.Host)
					} else {
						s.logger.Warn("won't report commit latency since broker coordinator is still unknown", zap.Int64("latencyMilliseconds", commitLatency.Milliseconds()))
					}

				})
			}
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
