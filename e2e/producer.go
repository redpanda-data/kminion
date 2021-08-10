package e2e

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type EndToEndMessage struct {
	MinionID  string `json:"minionID"`     // unique for each running kminion instance
	MessageID string `json:"messageID"`    // unique for each message
	Timestamp int64  `json:"createdUtcNs"` // when the message was created, unix nanoseconds

	partition  int  // used in message tracker
	hasArrived bool // used in tracker
}

func (m *EndToEndMessage) creationTime() time.Time {
	return time.Unix(0, m.Timestamp)
}

// Sends a EndToEndMessage to every partition
func (s *Service) produceLatencyMessages(ctx context.Context) {
	for i := 0; i < s.partitionCount; i++ {
		err := s.produceSingleMessage(ctx, i)
		if err != nil {
			s.logger.Error("failed to produce message to end-to-end topic",
				zap.String("topic_name", s.config.TopicManagement.Name),
				zap.Int("partition", i),
				zap.Error(err))
		}
	}
}

func (s *Service) produceSingleMessage(ctx context.Context, partition int) error {
	topicName := s.config.TopicManagement.Name
	record, msg := createEndToEndRecord(s.minionID, topicName, partition)

	startTime := time.Now()

	errCh := make(chan error)
	s.endToEndMessagesProducedInFlight.Inc()
	s.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		ackDuration := time.Since(startTime)
		s.endToEndMessagesProducedInFlight.Dec()
		s.endToEndMessagesProducedTotal.Inc()

		errCh <- err

		if err == nil {
			s.endToEndAckLatency.WithLabelValues(strconv.Itoa(int(r.Partition))).Observe(ackDuration.Seconds())
			s.messageTracker.addToTracker(msg)
		} else {
			s.endToEndMessagesProducedFailed.Inc()
		}
	})

	err := <-errCh
	if err != nil {
		s.logger.Error("error producing record", zap.Error(err))
		return err
	}
	return nil

}

func createEndToEndRecord(minionID string, topicName string, partition int) (*kgo.Record, *EndToEndMessage) {
	message := &EndToEndMessage{
		MinionID:  minionID,
		MessageID: uuid.NewString(),
		Timestamp: time.Now().UnixNano(),

		partition: partition,
	}

	mjson, err := json.Marshal(message)
	if err != nil {
		// Should never happen since the struct is so simple,
		// but if it does, something is completely broken anyway
		panic("cannot serialize EndToEndMessage")
	}

	record := &kgo.Record{
		Topic:     topicName,
		Value:     mjson,
		Partition: int32(partition), // we set partition for producing so our customPartitioner can make use of it
	}

	return record, message
}
