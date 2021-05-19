package e2e

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) startConsumeMessages(ctx context.Context) {
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

	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := client.PollFetches(ctx)
			receiveTimestamp := time.Now()

			// Log all errors and continue afterwards as we might get errors and still have some fetch results
			errors := fetches.Errors()
			for _, err := range errors {
				s.logger.Error("kafka fetch error",
					zap.String("topic", err.Topic),
					zap.Int32("partition", err.Partition),
					zap.Error(err.Err))
			}

			// Process messages
			fetches.EachRecord(func(record *kgo.Record) {
				if record != nil {
					s.processMessage(record, receiveTimestamp)
				}
			})
		}
	}
}

func (s *Service) commitOffsets(ctx context.Context) {
	client := s.client

	//
	// Commit offsets for processed messages
	// todo: the normal way to commit offsets with franz-go is pretty good, but in our special case
	// 		 we want to do it manually, seperately for each partition, so we can track how long it took
	if uncommittedOffset := client.UncommittedOffsets(); uncommittedOffset != nil {

		startCommitTimestamp := time.Now()

		client.CommitOffsets(ctx, uncommittedOffset, func(req *kmsg.OffsetCommitRequest, r *kmsg.OffsetCommitResponse, err error) {
			// got commit response
			latency := time.Since(startCommitTimestamp)

			if err != nil {
				s.logger.Error("offset commit failed", zap.Error(err), zap.Int64("latencyMilliseconds", latency.Milliseconds()))
				return
			}

			for _, t := range r.Topics {
				for _, p := range t.Partitions {
					err := kerr.ErrorForCode(p.ErrorCode)
					if err != nil {
						s.logger.Error("error committing partition offset", zap.String("topic", t.Topic), zap.Int32("partitionId", p.Partition), zap.Error(err))
					}
				}
			}

			// only report commit latency if the coordinator wasn't set too long ago
			if time.Since(s.clientHooks.lastCoordinatorUpdate) < 10*time.Second {
				coordinator := s.clientHooks.currentCoordinator.Load().(kgo.BrokerMetadata)
				s.onOffsetCommit(coordinator.NodeID, latency)
			}
		})
	}
}

// processMessage:
// - deserializes the message
// - checks if it is from us, or from another kminion process running somewhere else
// - hands it off to the service, which then reports metrics on it
func (s *Service) processMessage(record *kgo.Record, receiveTimestamp time.Time) {
	var msg EndToEndMessage
	if jerr := json.Unmarshal(record.Value, &msg); jerr != nil {
		return // maybe older version
	}

	if msg.MinionID != s.minionID {
		return // not from us
	}

	// restore partition, which was not serialized
	msg.partition = int(record.Partition)

	created := msg.creationTime()
	latency := receiveTimestamp.Sub(created)

	s.onRoundtrip(record.Partition, latency)

	// notify the tracker that the message arrived
	s.messageTracker.onMessageArrived(&msg)
}
