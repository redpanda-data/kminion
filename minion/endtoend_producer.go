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
	if err != nil {
		return nil, err
	}
	// This function abstract the producing message to all partitions
	err = s.kafkaSvc.Client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}
	})

	if err != nil {
		return nil, err
	}

	return &minionID, nil
}

func newManagementTopicRecord(topicName string, minionID string) (*kgo.Record, error) {

	timestamp := time.Now().UnixNano() / 1000000
	message := TopicManagementRecord{
		MinionID:  minionID,
		Timestamp: timestamp,
	}
	mjson, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	record := &kgo.Record{Topic: topicName, Value: []byte(mjson)}

	return record, nil
}
