package e2e

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) startConsumeMessages(ctx context.Context, initializedCh chan<- bool) {
	client := s.client

	s.logger.Info("Starting to consume end-to-end topic",
		zap.String("topic_name", s.config.TopicManagement.Name),
		zap.String("group_id", s.groupId))

	isInitialized := false
	for {
		fetches := client.PollFetches(ctx)
		if !isInitialized {
			isInitialized = true
			initializedCh <- true
		}

		// Log all errors and continue afterwards as we might get errors and still have some fetch results
		errors := fetches.Errors()
		for _, err := range errors {
			s.logger.Error("kafka fetch error",
				zap.String("topic", err.Topic),
				zap.Int32("partition", err.Partition),
				zap.Error(err.Err))
		}

		fetches.EachRecord(s.processMessage)
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
func (s *Service) processMessage(record *kgo.Record) {
	if record.Value == nil {
		// Init messages have nil values - we want to skip these. They are only used to make sure a consumer is ready.
		return
	}

	var msg EndToEndMessage
	if jerr := json.Unmarshal(record.Value, &msg); jerr != nil {
		s.logger.Error("failed to unmarshal message value", zap.Error(jerr))
		return // maybe older version
	}

	if msg.MinionID != s.minionID {
		return // not from us
	}

	// restore partition, which is not serialized
	msg.partition = int(record.Partition)
	s.messageTracker.onMessageArrived(&msg)
}
