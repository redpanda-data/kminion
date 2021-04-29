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
	topicName := s.Cfg.EndToEnd.TopicManagement.Name
	topic := kgo.ConsumeTopics(kgo.NewOffset().AtEnd(), topicName)
	balancer := kgo.Balancers(kgo.CooperativeStickyBalancer()) // Default GroupBalancer
	switch s.Cfg.EndToEnd.Consumer.RebalancingProtocol {
	case RoundRobin:
		balancer = kgo.Balancers(kgo.RoundRobinBalancer())
	case Range:
		balancer = kgo.Balancers(kgo.RangeBalancer())
	case Sticky:
		balancer = kgo.Balancers(kgo.StickyBalancer())
	}
	client.AssignPartitions(topic)

	// todo: use minionID as part of group id
	//
	client.AssignGroup(s.Cfg.EndToEnd.Consumer.GroupId, kgo.GroupTopics(topicName), balancer, kgo.DisableAutoCommit())
	s.logger.Info("Starting to consume " + topicName)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := client.PollRecords(ctx, 10)
			errors := fetches.Errors()
			for _, err := range errors {
				// Log all errors and continue afterwards as we might get errors and still have some fetch results
				s.logger.Error("kafka fetch error",
					zap.String("topic", err.Topic),
					zap.Int32("partition", err.Partition),
					zap.Error(err.Err))
			}

			receiveTimestamp := timeNowMs()

			//
			// Process messages
			iter := fetches.RecordIter()
			var record *kgo.Record
			for !iter.Done() {
				record = iter.Next()

				if record == nil {
					continue
				}

				s.processMessage(record, receiveTimestamp)
			}

			//
			// Commit offsets for processed messages
			// todo:
			// - do we need to keep track of what offset to commit for which partition??
			if uncommittedOffset := client.UncommittedOffsets(); uncommittedOffset != nil {

				startCommitTimestamp := timeNowMs()

				client.CommitOffsets(ctx, uncommittedOffset, func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
					// got commit response
					if err != nil {
						s.logger.Error(fmt.Sprintf("record had an error on commit: %v\n", err))
						s.setCachedItem("end_to_end_consumer_offset_availability", false, 120*time.Second)
					} else {
						commitLatencySec := float64(timeNowMs()-startCommitTimestamp) / float64(1000)
						s.endToEndCommitLatency.Observe(commitLatencySec)
						s.endToEndMessagesCommitted.Inc()

						if commitLatencySec <= s.Cfg.EndToEnd.Consumer.CommitSla.Seconds() {
							s.endToEndWithinCommitSla.Set(1)
						} else {
							s.endToEndWithinCommitSla.Set(0)
						}
					}
				})
			}
		}
	}

}

// todo: extract whole end-to-end feature into its own package
// todo: then also create a "tracker" that knows about in-flight messages, and the latest successful roundtrips

// processMessage takes a message and:
// - checks if it matches minionID and latency
// - updates metrics accordingly
func (s *Service) processMessage(record *kgo.Record, receiveTimestamp int64) {
	var msg EndToEndMessage
	if jerr := json.Unmarshal(record.Value, &msg); jerr != nil {
		return // maybe older version
	}

	if msg.MinionID != s.minionID {
		return // not from us
	}

	if msg.Timestamp < s.lastRoundtripTimestamp {
		return // msg older than what we recently processed (out of order, should never happen)
	}

	latencyMs := receiveTimestamp - msg.Timestamp
	if latencyMs > s.Cfg.EndToEnd.Consumer.RoundtripSla.Milliseconds() {
		s.endToEndWithinRoundtripSla.Set(0)
		return // too late!
	}

	s.lastRoundtripTimestamp = msg.Timestamp
	s.endToEndMessagesReceived.Inc()
	s.endToEndRoundtripLatency.Observe(float64(latencyMs) / 1000)
}
