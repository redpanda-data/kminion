package minion

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
)

func (s *Service) ListOffsetsCached(ctx context.Context, timestamp int64) (kadm.ListedOffsets, error) {
	reqId, ok := ctx.Value(RequestIDKey).(string)
	if !ok || reqId == "" {
		reqId = "default"
	}
	key := "partition-offsets-" + strconv.Itoa(int(timestamp)) + "-" + reqId

	if cachedRes, exists := s.getCachedItem(key); exists {
		return cachedRes.(kadm.ListedOffsets), nil
	}

	res, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		offsets, err := s.ListOffsets(ctx, timestamp)
		if err != nil {
			return nil, err
		}

		s.setCachedItem(key, offsets, 120*time.Second)

		return offsets, nil
	})
	if err != nil {
		return nil, err
	}

	return res.(kadm.ListedOffsets), nil
}

// ListOffsets fetches the low (timestamp: -2) or high water mark (timestamp: -1) for all topic partitions
func (s *Service) ListOffsets(ctx context.Context, timestamp int64) (kadm.ListedOffsets, error) {
	listedOffsets, err := s.admClient.ListEndOffsets(ctx)
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) {
			return nil, fmt.Errorf("failed to list offsets: %w", err)
		}

		if se.AllFailed {
			return nil, fmt.Errorf("failed to list offsets, all shard responses failed: %w", err)
		}
		s.logger.Info("failed to list offset from some shards", zap.Int("failed_shards", len(se.Errs)))
		for _, shardErr := range se.Errs {
			s.logger.Warn("shard error for listing end offsets",
				zap.Int32("broker_id", shardErr.Broker.NodeID),
				zap.Error(shardErr.Err))
		}
	}

	// Log inner errors before returning them. We do that inside of this function to avoid duplicate logging as the response
	// are cached for each scrape anyways.
	//
	// Create two metrics to aggregate error logs in few messages. Logging one message per occured partition error
	// is too much. Typical errors are LEADER_NOT_AVAILABLE etc.
	errorCountByErrCode := make(map[error]int)
	errorCountByTopic := make(map[string]int)

	// Iterate on all partitions
	listedOffsets.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil {
			errorCountByTopic[offset.Topic]++
			errorCountByErrCode[offset.Err]++
		}
	})

	// Print log line for each error type
	for err, count := range errorCountByErrCode {
		s.logger.Warn("failed to list some partitions watermarks",
			zap.Error(err),
			zap.Int("error_count", count))
	}
	if len(errorCountByTopic) > 0 {
		s.logger.Warn("some topics had one or more partitions whose watermarks could not be fetched from Kafka",
			zap.Int("topics_with_errors", len(errorCountByTopic)))
	}

	return listedOffsets, nil
}
