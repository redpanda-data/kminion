package minion

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// startConsumingOffsets consumes the __consumer_offsets topic and forwards the kafka messages to their respective
// methods where they'll be decoded and further processed.
func (s *Service) startConsumingOffsets(ctx context.Context) {
	client := s.kafkaSvc.Client
	topic := kgo.ConsumeTopics(kgo.NewOffset().AtStart(), "__consumer_offsets")
	client.AssignPartitions(topic)

	s.logger.Info("starting to consume messages from offsets topic")
	// TODO: Add select case for context cancellation to propagate signals / stopping the exporter
	for {
		fetches := client.PollFetches(ctx)
		errors := fetches.Errors()
		for _, err := range errors {
			// Log all errors and continue afterwards as we might get errors and still have some fetch results
			s.logger.Error("failed to fetch records from kafka",
				zap.String("topic", err.Topic),
				zap.Int32("partition", err.Partition),
				zap.Error(err.Err))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			err := s.decodeOffsetRecord(record)
			if err != nil {
				s.logger.Warn("failed to decode offset record", zap.Error(err))
			}
		}
	}
}

// decodeOffsetRecord decodes all messages in the consumer offsets topic by routing records to the correct decoding
// method.
func (s *Service) decodeOffsetRecord(record *kgo.Record) error {
	if len(record.Key) < 2 {
		return fmt.Errorf("offset commit key is supposed to be at least 2 bytes long")
	}
	messageVer := (&kbin.Reader{Src: record.Key}).Int16()

	switch messageVer {
	case 0, 1:
		err := s.decodeOffsetCommit(record)
		if err != nil {
			return err
		}
	case 2:
		err := s.decodeOffsetMetadata(record)
		if err != nil {
			return err
		}
	}

	return nil
}

// decodeOffsetMetadata decodes to metadata which includes the following information:
// - group
// - protocolType (connect/consumer/...)
// - generation
// - protocol
// - currentStateTimestamp
// - groupMembers (member metadata such aus: memberId, groupInstanceId, clientId, clientHost, rebalanceTimeout, ...)
func (s *Service) decodeOffsetMetadata(record *kgo.Record) error {
	childLogger := s.logger.With(
		zap.String("topic", record.Topic),
		zap.Int32("partition_id", record.Partition),
		zap.Int64("offset", record.Offset))

	metadataKey := kmsg.NewGroupMetadataKey()
	err := metadataKey.ReadFrom(record.Key)
	if err != nil {
		childLogger.Warn("failed to decode offset metadata key", zap.Error(err))
		return fmt.Errorf("failed to decode offset metadata key: %w", err)
	}

	if record.Value == nil {
		return nil
	}
	metadataValue := kmsg.NewGroupMetadataValue()
	err = metadataValue.ReadFrom(record.Value)
	if err != nil {
		childLogger.Warn("failed to decode offset metadata value", zap.Error(err))
		return fmt.Errorf("failed to decode offset metadata value: %w", err)
	}

	return nil
}

// decodeOffsetCommit decodes to group offsets which include the following information:
// - group, topic, partition
// - offset
// - leaderEpoch
// - metadata (user specified string for each offset commit)
// - commitTimestamp
// - expireTimestamp (only version 1 offset commits / deprecated)
func (s *Service) decodeOffsetCommit(record *kgo.Record) error {
	childLogger := s.logger.With(
		zap.String("topic", record.Topic),
		zap.Int32("partition_id", record.Partition),
		zap.Int64("offset", record.Offset))
	offsetCommitKey := kmsg.NewOffsetCommitKey()
	err := offsetCommitKey.ReadFrom(record.Key)
	if err != nil {
		childLogger.Warn("failed to decode offset commit key", zap.Error(err))
		return fmt.Errorf("failed to decode offset commit key: %w", err)
	}

	if record.Value == nil {
		// Tombstone
		return nil
	}

	offsetCommitValue := kmsg.NewOffsetCommitValue()
	err = offsetCommitValue.ReadFrom(record.Value)
	if err != nil {
		childLogger.Warn("failed to decode offset commit value", zap.Error(err))
		return fmt.Errorf("failed to decode offset commit value: %w", err)
	}
	s.storage.addOffsetCommit(offsetCommitKey, offsetCommitValue)

	return nil
}
