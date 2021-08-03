package e2e

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) startConsumeMessages(ctx context.Context) {
	client := s.client
	s.logger.Info("Starting to consume end-to-end topic",
		zap.String("topic_name", s.config.TopicManagement.Name),
		zap.String("group_id", s.groupId))

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
				s.processMessage(record, receiveTimestamp)
			})
		}
	}
}

func (s *Service) commitOffsets(ctx context.Context) {
	client := s.client
	uncommittedOffset := client.UncommittedOffsets()
	if uncommittedOffset == nil {
		return
	}

	startCommitTimestamp := time.Now()
	client.CommitOffsets(ctx, uncommittedOffset, func(_ *kgo.Client, req *kmsg.OffsetCommitRequest, r *kmsg.OffsetCommitResponse, err error) {
		// Got commit response
		latency := time.Since(startCommitTimestamp)

		if s.logCommitErrors(r, err) > 0 {
			return
		}

		// only report commit latency if the coordinator wasn't set too long ago
		if time.Since(s.clientHooks.lastCoordinatorUpdate) < 10*time.Second {
			coordinator := s.clientHooks.currentCoordinator.Load().(kgo.BrokerMetadata)
			s.onOffsetCommit(coordinator.NodeID, latency)
		}
	})
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
