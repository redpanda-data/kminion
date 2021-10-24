package minion

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Service) ListOffsetsCached(ctx context.Context, timestamp int64) (*kmsg.ListOffsetsResponse, error) {
	reqId := ctx.Value("requestId").(string)
	key := "partition-offsets-" + strconv.Itoa(int(timestamp)) + "-" + reqId

	if cachedRes, exists := s.getCachedItem(key); exists {
		return cachedRes.(*kmsg.ListOffsetsResponse), nil
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

	return res.(*kmsg.ListOffsetsResponse), nil
}

// ListOffsets fetches the low (timestamp: -2) or high water mark (timestamp: -1) for all topic partitions
func (s *Service) ListOffsets(ctx context.Context, timestamp int64) (*kmsg.ListOffsetsResponse, error) {
	metadata, err := s.GetMetadataCached(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	topicReqs := make([]kmsg.ListOffsetsRequestTopic, len(metadata.Topics))
	for i, topic := range metadata.Topics {
		req := kmsg.NewListOffsetsRequestTopic()
		req.Topic = *topic.Topic

		partitionReqs := make([]kmsg.ListOffsetsRequestTopicPartition, len(topic.Partitions))
		for j, partition := range topic.Partitions {
			partitionReqs[j] = kmsg.NewListOffsetsRequestTopicPartition()
			partitionReqs[j].Partition = partition.Partition
			partitionReqs[j].Timestamp = timestamp
		}
		req.Partitions = partitionReqs

		topicReqs[i] = req
	}

	req := kmsg.NewListOffsetsRequest()
	req.Topics = topicReqs

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return res, err
	}

	// Log inner errors before returning them. We do that inside of this function to avoid duplicate logging as the response
	// are cached for each scrape anyways.
	//
	// Create two metrics to aggregate error logs in few messages. Logging one message per occured partition error
	// is too much. Typical errors are LEADER_NOT_AVAILABLE etc.
	errorCountByErrCode := make(map[int16]int)
	errorCountByTopic := make(map[string]int)

	// Iterate on all partitions
	for _, topic := range res.Topics {
		for _, partition := range topic.Partitions {
			err := kerr.TypedErrorForCode(partition.ErrorCode)
			if err != nil {
				errorCountByErrCode[partition.ErrorCode]++
				errorCountByTopic[topic.Topic]++
			}
		}
	}

	// Print log line for each error type
	for errCode, count := range errorCountByErrCode {
		typedErr := kerr.TypedErrorForCode(errCode)
		s.logger.Warn("failed to list some partitions watermarks",
			zap.Error(typedErr),
			zap.Int("error_count", count))
	}
	if len(errorCountByTopic) > 0 {
		s.logger.Warn("some topics had one or more partitions whose watermarks could not be fetched from Kafka",
			zap.Int("topics_with_errors", len(errorCountByTopic)))
	}

	return res, nil
}
