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

// produceMessagesToAllPartitions sends an EndToEndMessage to every partition on the given topic
func (s *Service) produceMessagesToAllPartitions(ctx context.Context) {
	for i := 0; i < s.partitionCount; i++ {
		s.produceMessage(ctx, i)
	}
}

// produceMessage produces an end to end record to a single given partition. If it succeeds producing the record
// it will add it to the message tracker. If producing fails a message will be logged and the respective metrics
// will be incremented.
func (s *Service) produceMessage(ctx context.Context, partition int) {
	topicName := s.config.TopicManagement.Name
	record, msg := createEndToEndRecord(s.minionID, topicName, partition)

	startTime := time.Now()

	// This childCtx will ensure that we will abort our efforts to produce (including retries) when we exceed
	// the SLA for producers.
	childCtx, cancel := context.WithTimeout(ctx, s.config.Producer.AckSla+2*time.Second)

	pID := strconv.Itoa(partition)
	s.messagesProducedInFlight.WithLabelValues(pID).Inc()
	s.messageTracker.addToTracker(msg)
	s.client.Produce(childCtx, record, func(r *kgo.Record, err error) {
		defer cancel()
		ackDuration := time.Since(startTime)
		s.messagesProducedInFlight.WithLabelValues(pID).Dec()
		s.messagesProducedTotal.WithLabelValues(pID).Inc()
		// We add 0 in order to ensure that the "failed" metric series for that partition id are initialized as well.
		s.messagesProducedFailed.WithLabelValues(pID).Add(0)
		s.lostMessages.WithLabelValues(pID).Add(0)

		if err != nil {
			s.messagesProducedFailed.WithLabelValues(pID).Inc()
			s.messageTracker.removeFromTracker(msg.MessageID)

			s.logger.Info("failed to produce message to end-to-end topic",
				zap.String("topic_name", r.Topic),
				zap.Int32("partition", r.Partition),
				zap.Error(err))
			return
		} else {
			// Update the message's state. If this message expires and is marked as successfully produced we will
			// report this as a lost message, which would indicate that the producer was told that the message got
			// produced successfully, but it got lost somewhere.
			// We need to use updateItemIfExists() because it's possible that the message has already been consumed
			// before we have received the message here (because we were awaiting the produce ack).
			msg.state = EndToEndMessageStateProducedSuccessfully
			msg.produceLatency = ackDuration.Seconds()

			// TODO: Enable again as soon as https://github.com/jellydatora/ttlcache/issues/60 is fixed
			// Because we cannot update cache items in an atomic fashion we currently can't use this method
			// as this would cause a race condition which ends up in records being reported as lost/expired.
			// s.messageTracker.updateItemIfExists(msg)
		}

		s.produceLatency.WithLabelValues(pID).Observe(ackDuration.Seconds())
	})
}

func createEndToEndRecord(minionID string, topicName string, partition int) (*kgo.Record, *EndToEndMessage) {
	message := &EndToEndMessage{
		MinionID:  minionID,
		MessageID: uuid.NewString(),
		Timestamp: time.Now().UnixNano(),

		partition: partition,
		state:     EndToEndMessageStateCreated,
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
