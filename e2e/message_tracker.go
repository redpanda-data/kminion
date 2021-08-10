package e2e

import (
	"time"

	goCache "github.com/patrickmn/go-cache"
	"go.uber.org/zap"
)

// messageTracker keeps track of messages
//
// When we successfully send a mesasge, it will be added to this tracker.
// Later, when we receive the message back in the consumer, the message is marked as completed and removed from the tracker.
// If the message does not arrive within the configured `consumer.roundtripSla`, it is counted as lost.
// A lost message is reported in the `roundtrip_latency_seconds` metric with infinite duration.
// We use a dedicated counter to track messages that couldn't be
// produced to Kafka.
//
// When we fail to send a message, it isn't tracked.
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

func (t *messageTracker) onMessageArrived(arrivedMessage *EndToEndMessage) {
	cachedMessageInterface, _, found := t.cache.GetWithExpiration(arrivedMessage.MessageID)
	if !found {
		// message expired and was removed from the cache
		// it arrived too late, nothing to do here...
		return
	}

	actualExpireTime := arrivedMessage.creationTime().Add(t.svc.config.Consumer.RoundtripSla)
	if time.Now().Before(actualExpireTime) {
		// message arrived early enough

		// timeUntilExpire := time.Until(actualExpireTime)
		// t.logger.Debug("message arrived",
		// 	zap.Duration("timeLeft", timeUntilExpire),
		// 	zap.Duration("age", ),
		// 	zap.Int("partition", msg.partition),
		// 	zap.String("messageId", msg.MessageID),
		// )
	} else {
		// Message arrived late, but was still in cache.
		// Maybe we could log something like "message arrived after the sla"...
		//
		// But for now we don't report it as "lost" in the log (because it actually *did* arrive just now, just too late).
		// The metrics will report it as 'duration infinite' anyway.
	}

	// Set it as arrived, so we don't log it as lost in 'onMessageExpired' and remove it from the tracker
	msg := cachedMessageInterface.(*EndToEndMessage)
	msg.hasArrived = true
	t.cache.Delete(msg.MessageID)
}

func (t *messageTracker) onMessageExpired(_ string, msg *EndToEndMessage) {
	created := msg.creationTime()
	age := time.Since(created)
	t.svc.lostMessages.Inc()

	t.logger.Debug("message lost/expired",
		zap.Int64("age_ms", age.Milliseconds()),
		zap.Int("partition", msg.partition),
		zap.String("message_id", msg.MessageID),
	)
}
