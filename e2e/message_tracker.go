package e2e

import (
	"strconv"
	"time"

	goCache "github.com/patrickmn/go-cache"
	"go.uber.org/zap"
)

// messageTracker keeps track of the messages' lifetime
//
// When we successfully send a mesasge, it will be added to this tracker.
// Later, when we receive the message back in the consumer, the message is marked as completed and removed from the tracker.
// If the message does not arrive within the configured `consumer.roundtripSla`, it is counted as lost. Messages that
// failed to be produced will not be
// considered as lost message.
//
// We use a dedicated counter to track messages that couldn't be  produced to Kafka.
type messageTracker struct {
	svc    *Service
	logger *zap.Logger
	cache  *goCache.Cache
}

func newMessageTracker(svc *Service) *messageTracker {
	defaultExpirationTime := svc.config.Consumer.RoundtripSla
	cleanupInterval := 1 * time.Second

	t := &messageTracker{
		svc:    svc,
		logger: svc.logger.Named("message_tracker"),
		cache:  goCache.New(defaultExpirationTime, cleanupInterval),
	}

	t.cache.OnEvicted(func(key string, item interface{}) {
		t.onMessageExpired(key, item.(*EndToEndMessage))
	})

	return t
}

func (t *messageTracker) addToTracker(msg *EndToEndMessage) {
	t.cache.SetDefault(msg.MessageID, msg)
}

func (t *messageTracker) removeFromTracker(messageID string) {
	t.cache.Delete(messageID)
}

func (t *messageTracker) onMessageArrived(arrivedMessage *EndToEndMessage) {
	cm, found := t.cache.Get(arrivedMessage.MessageID)
	if !found {
		// message expired and was removed from the cache
		// it arrived too late, nothing to do here...
		return
	}

	msg := cm.(*EndToEndMessage)

	expireTime := arrivedMessage.creationTime().Add(t.svc.config.Consumer.RoundtripSla)
	isOnTime := time.Now().Before(expireTime)
	latency := time.Now().Sub(msg.creationTime())

	if !isOnTime {
		// Message arrived late, but was still in cache. We don't increment the lost counter here because eventually
		// it will be evicted from the cache. This case should only pop up if the sla time is exceeded, but if the
		// item has not been evicted from the cache yet (because we clean it only every second).
		t.logger.Info("message arrived late, will be marked as a lost message",
			zap.Int64("delay_ms", latency.Milliseconds()),
			zap.String("id", msg.MessageID))
		return
	}

	// message arrived early enough
	pID := strconv.Itoa(msg.partition)
	t.svc.messagesReceived.WithLabelValues(pID).Inc()
	t.svc.roundtripLatency.WithLabelValues(pID).Observe(latency.Seconds())

	// We mark the message as arrived so that we won't mark the message as lost and overwrite that modified message
	// into the cache.
	msg.hasArrived = true
	t.cache.Set(msg.MessageID, msg, 0)
	t.cache.Delete(msg.MessageID)
}

func (t *messageTracker) onMessageExpired(_ string, msg *EndToEndMessage) {
	// Because `t.cache.Delete` will invoke the onEvicted method we have to expect some calls to this function
	// even though messages have arrived. Thus, we quit early if we receive such a message.
	if msg.hasArrived || msg.failedToProduce {
		return
	}

	created := msg.creationTime()
	age := time.Since(created)
	t.svc.lostMessages.WithLabelValues(strconv.Itoa(msg.partition)).Inc()

	t.logger.Info("message lost/expired",
		zap.Int64("age_ms", age.Milliseconds()),
		zap.Int("partition", msg.partition),
		zap.String("message_id", msg.MessageID),
	)
}
