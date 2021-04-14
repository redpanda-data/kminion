package minion

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) ConsumeFromManagementTopic(ctx context.Context) error {
	client := s.kafkaSvc.Client
	topicMessage := s.Cfg.EndToEnd.TopicManagement.Name
	topic := kgo.ConsumeTopics(kgo.NewOffset().AtEnd(), topicMessage)
	balancer := kgo.Balancers(kgo.CooperativeStickyBalancer())
	switch s.Cfg.EndToEnd.Consumer.RebalancingProtocol {
	case RoundRobin:
		balancer = kgo.Balancers(kgo.RoundRobinBalancer())
	case Range:
		balancer = kgo.Balancers(kgo.RangeBalancer())
	case Sticky:
		balancer = kgo.Balancers(kgo.StickyBalancer())
	}
	client.AssignPartitions(topic)
	client.AssignGroup(s.Cfg.EndToEnd.Consumer.GroupId, kgo.GroupTopics(topicMessage), balancer)
	s.logger.Info("starting to consume topicManagement")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			startConsumeTimestamp := timeNowMs()
			fetches := client.PollFetches(ctx)
			errors := fetches.Errors()
			for _, err := range errors {
				// Log all errors and continue afterwards as we might get errors and still have some fetch results
				s.logger.Error("failed to fetch records from kafka",
					zap.String("topic", err.Topic),
					zap.Int32("partition", err.Partition),
					zap.Error(err.Err))
			}

			startConsumeTimestamp := timeNowMs()
			iter := fetches.RecordIter()
			var record *kgo.Record
			for !iter.Done() {
				record = iter.Next()

				if record == nil {
					continue
				}

				res := TopicManagementRecord{}
				json.Unmarshal(record.Value, &res)

				// Push the latency to endtoendLatencies that will be consumed by prometheus later
				latencySec := float64(timeNowMs()-res.Timestamp) / float64(1000)
				s.observeLatencyHistogram(latencySec, int(record.Partition))
				s.storage.markRecordConsumed(record)
				uncommittedOffset := client.UncommittedOffsets()
				// Only commit if uncommittedOffset return value
				if uncommittedOffset != nil {
					startCommitTimestamp := timeNowMs()
					client.CommitOffsets(ctx, uncommittedOffset, func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
						if err != nil {
							if err != kgo.ErrNoDial {
								s.logger.Error(fmt.Sprintf("record had an error on commit: %v\n", err))
							}
							s.setCachedItem("end_to_end_consumer_offset_availability", false, 120*time.Second)
						} else {
							commitLatencySec := float64(timeNowMs()-startCommitTimestamp) / float64(1000)
							s.observeCommitLatencyHistogram(commitLatencySec, int(record.Partition))
							s.setCachedItem("end_to_end_consumer_offset_availability", true, 120*time.Second)
						}
					})
				}
			}
			s.setCachedItem("end_to_end_consume_duration", timeNowMs()-startConsumeTimestamp, 120*time.Second)
		}
	}

}

func (s *Service) ConsumeDurationMs(ctx context.Context) (int64, bool) {
	ms, exists := s.getCachedItem("end_to_end_consume_duration")
	if exists {
		return ms.(int64), true
	}
	return 0, false
}

func (s *Service) OffsetCommitAvailability(ctx context.Context) bool {
	ok, exists := s.getCachedItem("end_to_end_consumer_offset_availability")
	if !exists {
		return false
	}
	return ok.(bool)
}
