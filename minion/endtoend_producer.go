package minion

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicManagementRecord struct {
	MinionID  string `json:"minionID"`
	Timestamp int64  `json:"timestamp"`
}

func (s *Service) produceToManagementTopic(ctx context.Context) error {

	topicName := s.Cfg.EndToEnd.TopicManagement.Name

	record, err := createEndToEndRecord(topicName, s.minionID)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			startTime := timeNowMs()
			s.endToEndMessagesProduced.Inc()

			err = s.kafkaSvc.Client.Produce(ctx, record, func(r *kgo.Record, err error) {
				endTime := timeNowMs()
				ackDuration := endTime - startTime

				if err != nil {
					fmt.Printf("record had a produce error: %v\n", err)
				} else {
					s.endToEndMessagesAcked.Inc()

					if ackDuration < s.Cfg.EndToEnd.Producer.AckSla.Milliseconds() {
						s.endToEndWithinRoundtripSla.Set(1)
					} else {
						s.endToEndWithinRoundtripSla.Set(0)
					}
				}
			})

			if err != nil {
				return err
			}
			return nil
		}
	}

}

func createEndToEndRecord(topicName string, minionID string) (*kgo.Record, error) {

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
