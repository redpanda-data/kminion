package minion

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
)

func (s *Service) ListEndOffsetsCached(ctx context.Context) (kadm.ListedOffsets, error) {
	return s.listOffsetsCached(ctx, "end")
}

func (s *Service) ListStartOffsetsCached(ctx context.Context) (kadm.ListedOffsets, error) {
	return s.listOffsetsCached(ctx, "start")
}

func (s *Service) listOffsetsCached(ctx context.Context, offsetType string) (kadm.ListedOffsets, error) {
	reqId := ctx.Value("requestId").(string)
	key := fmt.Sprintf("partition-%s-offsets-%s", offsetType, reqId)

	if cachedRes, exists := s.getCachedItem(key); exists {
		return cachedRes.(kadm.ListedOffsets), nil
	}

	var listFunc func(context.Context) (kadm.ListedOffsets, error)
	switch offsetType {
	case "end":
		listFunc = s.ListEndOffsets
	case "start":
		listFunc = s.ListStartOffsets
	default:
		return nil, fmt.Errorf("invalid offset type: %s", offsetType)
	}

	res, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		offsets, err := listFunc(ctx)
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

// ListEndOffsets fetches the high water mark for all topic partitions.
func (s *Service) ListEndOffsets(ctx context.Context) (kadm.ListedOffsets, error) {
	return s.listOffsetsInternal(ctx, s.admClient.ListEndOffsets, "end")
}

// ListStartOffsets fetches the low water mark for all topic partitions.
func (s *Service) ListStartOffsets(ctx context.Context) (kadm.ListedOffsets, error) {
	return s.listOffsetsInternal(ctx, s.admClient.ListStartOffsets, "start")
}

type listOffsetsFunc func(context.Context, ...string) (kadm.ListedOffsets, error)

func (s *Service) listOffsetsInternal(ctx context.Context, listFunc listOffsetsFunc, offsetType string) (kadm.ListedOffsets, error) {
	listedOffsets, err := listFunc(ctx)
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) {
			return nil, fmt.Errorf("failed to list %s offsets: %w", offsetType, err)
		}

		if se.AllFailed {
			return nil, fmt.Errorf("failed to list %s offsets, all shard responses failed: %w", offsetType, err)
		}
		s.logger.Info(fmt.Sprintf("failed to list %s offset from some shards", offsetType), zap.Int("failed_shards", len(se.Errs)))
		for _, shardErr := range se.Errs {
			s.logger.Warn(fmt.Sprintf("shard error for listing %s offsets", offsetType),
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
		s.logger.Warn(fmt.Sprintf("failed to list some partitions %s watermarks", offsetType),
			zap.Error(err),
			zap.Int("error_count", count))
	}
	if len(errorCountByTopic) > 0 {
		s.logger.Warn(fmt.Sprintf("some topics had one or more partitions whose %s watermarks could not be fetched from Kafka", offsetType),
			zap.Int("topics_with_errors", len(errorCountByTopic)))
	}

	return listedOffsets, nil
}
