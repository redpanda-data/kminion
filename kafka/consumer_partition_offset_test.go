package kafka

import (
	"bytes"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestDecodeConsumerPartitionOffsetV3 is supposed to successfully decode an offset message
// which is encoded according to value version 3.
func TestDecodeConsumerPartitionOffsetV3(t *testing.T) {
	key := bytes.NewBuffer([]byte("\x00\nmy-group-5\x00\x06orders\x00\x00\x00\x00"))
	value := bytes.NewBuffer([]byte("\x00\x03\x00\x00\x00\x00\x00\x00@\x96\xff\xff\xff\xff\x00\x00\x00\x00\x01j\xfb\x8a\xb6\x16"))
	logger := log.WithFields(log.Fields{})

	expected := &ConsumerPartitionOffset{
		Group:     "my-group-5",
		Topic:     "orders",
		Partition: 0,
		Offset:    16534,
		Timestamp: 1558998332950,
	}

	offset, err := newConsumerPartitionOffset(key, value, logger)
	assert.Nil(t, err, "Expected newConsumerPartitionOffset to return no error")
	assert.Equal(t, expected, offset, "Decoded offset does not equal expected value")
}
