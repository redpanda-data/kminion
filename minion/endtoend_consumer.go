package minion

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func (s *Service) ConsumeFromManagementTopic(ctx context.Context) error {
	client := s.kafkaSvc.Client
	topicMessage := s.Cfg.EndToEnd.TopicManagement.Name
	topic := kgo.ConsumeTopics(kgo.NewOffset().AtStart(), topicMessage)
	client.AssignPartitions(topic)

	s.logger.Info("starting to consume topicManagement")

	for {
		select {
		case <-ctx.Done():
			return nil
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

				res := TopicManagementRecord{}
				json.Unmarshal(record.Value, &res)

				// Push the latency to endtoendLatencies that will be consumed by prometheus later
				latency := (time.Now().UnixNano() / 1000000) - res.Timestamp
				s.endtoendLatencies = append(s.endtoendLatencies, latency)

				s.storage.markRecordConsumed(record)
				return nil
			}
		}
	}
}

func (s *Service) GetAndResetEndToEndLatencies(ctx context.Context) []int64 {
	currentLatency := s.endtoendLatencies
	s.endtoendLatencies = []int64{}
	return currentLatency
}
