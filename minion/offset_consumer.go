package minion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// startConsumingOffsets consumes the __consumer_offsets topic and forwards the kafka messages to their respective
// methods where they'll be decoded and further processed.
func (s *Service) startConsumingOffsets(ctx context.Context) {
	client := s.client

	s.logger.Info("starting to consume messages from offsets topic")
	go s.checkIfConsumerLagIsCaughtUp(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
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
				s.storage.markRecordConsumed(record)

				err := s.decodeOffsetRecord(record)
				if err != nil {
					s.logger.Warn("failed to decode offset record", zap.Error(err))
				}
			}
		}
	}
}

// checkIfConsumerLagIsCaughtUp fetches the newest partition offsets for all partitions in the __consumer_offsets
// topic and compares these against the last consumed messages from our offset consumer. If the consumed offsets are
// higher than the partition offsets this means we caught up the initial lag and can mark our storage as ready. A ready
// store will start to expose consumer group offsets.
func (s *Service) checkIfConsumerLagIsCaughtUp(ctx context.Context) {
	for {
		time.Sleep(12 * time.Second)
		s.logger.Debug("checking if lag in consumer offsets topic is caught up")

		// 1. Get topic high watermarks for __consumer_offsets topic
		req := kmsg.NewMetadataRequest()
		topic := kmsg.NewMetadataRequestTopic()
		topicName := "__consumer_offsets"
		topic.Topic = &topicName
		req.Topics = []kmsg.MetadataRequestTopic{topic}

		res, err := req.RequestWith(ctx, s.client)
		if err != nil {
			s.logger.Warn("failed to check if consumer lag on offsets topic is caught up because metadata request failed",
				zap.Error(err))
			continue
		}

		// 2. Request high watermarks for consumer offset partitions
		topicReqs := make([]kmsg.ListOffsetsRequestTopic, len(res.Topics))
		for i, topic := range res.Topics {
			req := kmsg.NewListOffsetsRequestTopic()
			req.Topic = topic.Topic

			partitionReqs := make([]kmsg.ListOffsetsRequestTopicPartition, len(topic.Partitions))
			for j, partition := range topic.Partitions {
				partitionReqs[j] = kmsg.NewListOffsetsRequestTopicPartition()
				partitionReqs[j].Partition = partition.Partition
				partitionReqs[j].Timestamp = -1 // Newest
			}
			req.Partitions = partitionReqs

			topicReqs[i] = req
		}
		offsetReq := kmsg.NewListOffsetsRequest()
		offsetReq.Topics = topicReqs
		highMarksRes, err := offsetReq.RequestWith(ctx, s.client)
		if err != nil {
			s.logger.Warn("failed to check if consumer lag on offsets topic is caught up because high watermark request failed",
				zap.Error(err))
			continue
		}
		if len(highMarksRes.Topics) != 1 {
			s.logger.Error("expected exactly one topic response for high water mark request")
			continue
		}

		// 3. Check if high watermarks have been consumed. To avoid a race condition here we will wait some time before
		// comparing, so that the consumer has enough time to catch up to the new high watermarks we just fetched.
		time.Sleep(3 * time.Second)
		consumedOffsets := s.storage.getConsumedOffsets()
		topicRes := highMarksRes.Topics[0]
		isReady := true

		type laggingParition struct {
			Name string
			Id   int32
			Lag  int64
		}
		var partitionsLagging []laggingParition
		totalLag := int64(0)
		for _, partition := range topicRes.Partitions {
			err := kerr.ErrorForCode(partition.ErrorCode)
			if err != nil {
				s.logger.Warn("failed to check if consumer lag on offsets topic is caught up because high "+
					"watermark request failed, with an inner error",
					zap.Error(err))
			}

			highWaterMark := partition.Offset - 1
			consumedOffset := consumedOffsets[partition.Partition]
			partitionLag := highWaterMark - consumedOffset
			if partitionLag < 0 {
				partitionLag = 0
			}

			if partitionLag > 0 {
				partitionsLagging = append(partitionsLagging, laggingParition{
					Name: topicRes.Topic,
					Id:   partition.Partition,
					Lag:  partitionLag,
				})
				totalLag += partitionLag
				s.logger.Debug("consumer_offsets topic lag has not been caught up yet",
					zap.Int32("partition_id", partition.Partition),
					zap.Int64("high_water_mark", highWaterMark),
					zap.Int64("consumed_offset", consumedOffset),
					zap.Int64("partition_lag", partitionLag))
				isReady = false
				continue
			}
		}
		if isReady {
			s.logger.Info("successfully consumed all consumer offsets. consumer group lags will be exported from now on")
			s.storage.setReadyState(true)
			return
		} else {
			s.logger.Info("catching up the message lag on consumer offsets",
				zap.Int("lagging_partitions_count", len(partitionsLagging)),
				zap.Any("lagging_partitions", partitionsLagging),
				zap.Int64("total_lag", totalLag))
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
		// Tombstone - The group offset is expired or no longer valid (e.g. because the topic has been deleted)
		s.storage.deleteOffsetCommit(offsetCommitKey)
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

func (s *Service) GetNumberOfOffsetRecordsConsumed() float64 {
	return s.storage.getNumberOfConsumedRecords()
}
