package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestOffsetTombstone(t *testing.T) {
	storageCh := make(chan *StorageRequest, 1)
	mockConsumer := &OffsetConsumer{
		logger:         log.WithFields(log.Fields{}),
		storageChannel: storageCh,
	}

	// Tombstone message
	tombstone := &sarama.ConsumerMessage{
		Key:   []byte("\x00\x01\x00\x16console-consumer-36268\x00\naccess-log\x00\x00\x00\x10"),
		Value: []byte(""),
	}
	mockConsumer.processMessage(tombstone)
	result := <-storageCh
	assert.Equal(t, StorageDeleteConsumerGroup, result.RequestType, "Expected storage request to be a delete consumer group request")
}

func TestIsTopicAllowed(t *testing.T) {
	mockConsumer := &OffsetConsumer{
		logger: log.WithFields(log.Fields{}),
		options: &options.Options{
			IgnoreSystemTopics: true,
		},
	}

	table := []struct {
		input    string
		expected bool
	}{
		{"random-topic-name", true},
		{"_single-prefix", true},
		{"prefix__", true},
		{"__custom_internal_topic", false},
		{"_confluent_metrics", false},
	}

	for _, test := range table {
		t.Run(test.input, func(t *testing.T) {
			isAllowed := mockConsumer.isTopicAllowed(test.input)
			assert.Equalf(t, test.expected, isAllowed, "IsTopicAllowed returned %v instead of %v", isAllowed, test.expected)
		})
	}
}
