package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"testing"
)

func TestProcessOffsetCommit(t *testing.T) {
	mockConsumer := &OffsetConsumer{
		logger: log.WithFields(log.Fields{}),
	}

	// Tombstone message
	tombstone := &sarama.ConsumerMessage{
		Key:   []byte("\x00\x01\x00\x16console-consumer-36268\x00\naccess-log\x00\x00\x00\x10"),
		Value: []byte(""),
	}
	mockConsumer.processMessage(tombstone)
}
