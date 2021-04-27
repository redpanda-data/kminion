package minion

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicManagementRecord struct {
	MinionID  string `json:"minionID"`
	Timestamp int64  `json:"timestamp"`
}

func (s *Service) ProduceToManagementTopic(ctx context.Context) (*string, error) {

	topicName := s.Cfg.EndToEnd.TopicManagement.Name
	minionID := uuid.NewString()
	record, err := newManagementTopicRecord(topicName, minionID)
	produceCounts := s.ProduceCounts(ctx)
	produceAcks := s.ProduceAcks(ctx)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			startTime := timeNowMs()
			// This function abstract the producing message to all partitions
			s.setCachedItem("end_to_end_produce_counts", produceCounts+1, 120*time.Second)
			err = s.kafkaSvc.Client.Produce(ctx, record, func(r *kgo.Record, err error) {
				if err != nil {
					fmt.Printf("record had a produce error: %v\n", err)
				} else {
					s.setCachedItem("end_to_end_produce_acks", produceAcks+1, 120*time.Second)
					s.setCachedItem("end_to_end_produce_duration", timeNowMs()-startTime, 120*time.Second)
				}
			})

			if err != nil {
				return nil, err
			}
			return &minionID, nil
		}
	}

}

func newManagementTopicRecord(topicName string, minionID string) (*kgo.Record, error) {

	timestamp := timeNowMs()
	message := TopicManagementRecord{
		MinionID:  minionID,
		Timestamp: timestamp,
	}
	mjson, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	record := &kgo.Record{
		Topic: topicName,
		Key:   []byte(uuid.NewString()),
		Value: []byte(mjson),
	}

	return record, nil
}

func (s *Service) ProduceDurationMs(ctx context.Context) (int64, bool) {
	ms, exists := s.getCachedItem("end_to_end_produce_duration")
	if exists {
		return ms.(int64), true
	}
	return 0, false
}

func (s *Service) ProduceCounts(ctx context.Context) int64 {
	counts, exists := s.getCachedItem("end_to_end_produce_counts")
	if exists {
		return counts.(int64)
	}
	return 0
}

func (s *Service) ProduceAcks(ctx context.Context) int64 {
	acks, exists := s.getCachedItem("end_to_end_produce_acks")
	if exists {
		return acks.(int64)
	}
	return 0
}
