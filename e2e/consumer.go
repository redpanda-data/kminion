package e2e

import (
	"context"
	"encoding/json"
	"strconv"
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

	childCtx, cancel := context.WithTimeout(ctx, s.config.Consumer.CommitSla)
	client.CommitOffsets(childCtx, uncommittedOffset, func(_ *kgo.Client, req *kmsg.OffsetCommitRequest, r *kmsg.OffsetCommitResponse, err error) {
		cancel()

		coordinator := s.clientHooks.currentCoordinator.Load().(kgo.BrokerMetadata)
		coordinatorID := strconv.Itoa(int(coordinator.NodeID))

		latency := time.Since(startCommitTimestamp)
		s.offsetCommitLatency.WithLabelValues(coordinatorID).Observe(latency.Seconds())
		s.offsetCommitsTotal.WithLabelValues(coordinatorID).Inc()
		// We do this to ensure that a series with that coordinator id is initialized
		s.offsetCommitsTotal.WithLabelValues(coordinatorID).Add(0)

		// If we have at least one error in our commit response we want to report it as an error with an appropriate
		// reason as label.
		if errCode := s.logCommitErrors(r, err); errCode != "" {
			s.offsetCommitsFailedTotal.WithLabelValues(coordinatorID, errCode).Inc()
			return
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
