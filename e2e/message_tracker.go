package e2e

import (
	"fmt"
	"strconv"
	"time"

	"github.com/jellydator/ttlcache/v2"

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
	cache  *ttlcache.Cache
}

func newMessageTracker(svc *Service) *messageTracker {
	defaultExpirationDuration := svc.config.Consumer.RoundtripSla
	cache := ttlcache.NewCache()
	_ = cache.SetTTL(defaultExpirationDuration)

	t := &messageTracker{
		svc:    svc,
		logger: svc.logger.Named("message_tracker"),
		cache:  cache,
	}
	t.cache.SetExpirationReasonCallback(func(key string, reason ttlcache.EvictionReason, value interface{}) {
		t.onMessageExpired(key, reason, value.(*EndToEndMessage))
	})

	return t
}

func (t *messageTracker) addToTracker(msg *EndToEndMessage) {
	_ = t.cache.Set(msg.MessageID, msg)
}

// updateItemIfExists only updates a message if it still exists in the cache. The remaining time to live will not
// be refreshed.
// If it doesn't exist an ttlcache.ErrNotFound error will be returned.
//
//nolint:unused
func (t *messageTracker) updateItemIfExists(msg *EndToEndMessage) error {
	_, ttl, err := t.cache.GetWithTTL(msg.MessageID)
	if err != nil {
		if err == ttlcache.ErrNotFound {
			return err
		}
		panic(err)
	}

	// Because the returned TTL is set to the original TTL duration (and not the remaining TTL) we have to calculate
	// the remaining TTL now as we want to update the existing cache item without changing the remaining time to live.
	expiryTimestamp := msg.creationTime().Add(ttl)
	remainingTTL := time.Until(expiryTimestamp)
	if remainingTTL < 0 {
		// This entry should have been deleted already. Race condition.
		return ttlcache.ErrNotFound
	}

	err = t.cache.SetWithTTL(msg.MessageID, msg, remainingTTL)
	if err != nil {
		panic(err)
	}

	return nil
}

// removeFromTracker removes an entry from the cache. If the key does not exist it will return an ttlcache.ErrNotFound error.
func (t *messageTracker) removeFromTracker(messageID string) error {
	return t.cache.Remove(messageID)
}

func (t *messageTracker) onMessageArrived(arrivedMessage *EndToEndMessage) {
	cm, err := t.cache.Get(arrivedMessage.MessageID)
	if err != nil {
		if err == ttlcache.ErrNotFound {
			// message expired and was removed from the cache
			// it arrived too late, nothing to do here...
			return
		} else {
			panic(fmt.Errorf("failed to get message from cache: %w", err))
		}
	}

	msg := cm.(*EndToEndMessage)

	expireTime := msg.creationTime().Add(t.svc.config.Consumer.RoundtripSla)
	isExpired := time.Now().Before(expireTime)
	latency := time.Since(msg.creationTime())

	if !isExpired {
		// Message arrived late, but was still in cache. We don't increment the lost counter here because eventually
		// it will be evicted from the cache. This case should only pop up if the sla time is exceeded, but if the
		// item has not been evicted from the cache yet.
		t.logger.Info("message arrived late, will be marked as a lost message",
			zap.Int64("delay_ms", latency.Milliseconds()),
			zap.String("id", msg.MessageID))
		return
	}

	// message arrived early enough
	pID := strconv.Itoa(msg.partition)
	t.svc.messagesReceived.WithLabelValues(pID).Inc()
	t.svc.roundtripLatency.WithLabelValues(pID).Observe(latency.Seconds())

	// Remove message from cache, so that we don't track it any longer and won't mark it as lost when the entry expires.
	_ = t.cache.Remove(msg.MessageID)
}

func (t *messageTracker) onMessageExpired(_ string, reason ttlcache.EvictionReason, value interface{}) {
	if reason == ttlcache.Removed {
		// We are not interested in messages that have been removed by us!
		return
	}

	msg := value.(*EndToEndMessage)

	created := msg.creationTime()
	age := time.Since(created)
	t.svc.lostMessages.WithLabelValues(strconv.Itoa(msg.partition)).Inc()

	t.logger.Debug("message expired/lost",
		zap.Int64("age_ms", age.Milliseconds()),
		zap.Int("partition", msg.partition),
		zap.String("message_id", msg.MessageID),
		zap.Bool("successfully_produced", msg.state == EndToEndMessageStateProducedSuccessfully),
		zap.Float64("produce_latency_seconds", msg.produceLatency),
	)
}
