package e2e

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

/*
	This file defines a custom partitioner for use in franz-go.

	Why do we need one?
	Because we want to have control over which partition exactly a message is sent to,
	and the built-in partitioners in franz-go don't support that.

	Why do we want to do that?
	We want to test all brokers with our "end-to-end test" test (sending message, receiving it again, measuring latency).
	To do that, we need to ensure we send a message to each broker.


	todo:
	Of course that also requires that we have exactly as many partitions as we have brokers,
	and that each broker leads exactly one of our test partitions.

	However, we only create the topic initially (with the right amount of partitions and proper leader balancing over the brokers).
	So should two or more partitions of our topic end up being led (/hosted) by the same broker somehow, we neither detect nor fix that currently.
*/

// Partitioner: Creates a TopicPartitioner for a given topic name
type customPartitioner struct {
	logger                 *zap.Logger
	expectedPartitionCount int
}

func (c *customPartitioner) ForTopic(topicName string) kgo.TopicPartitioner {
	return &customTopicPartitioner{
		logger:                 c.logger,
		expectedPartitionCount: c.expectedPartitionCount,
	}
}

// TopicPartitioner: Determines which partition to produce a message to
type customTopicPartitioner struct {
	logger                 *zap.Logger
	expectedPartitionCount int
}

// OnNewBatch is called when producing a record if that record would
// trigger a new batch on its current partition.
func (c *customTopicPartitioner) OnNewBatch() {
	// Not interesting for us
}

// RequiresConsistency returns true if a record must hash to the same
// partition even if a partition is down.
// If true, a record may hash to a partition that cannot be written to
// and will error until the partition comes back.
func (c *customTopicPartitioner) RequiresConsistency(_ *kgo.Record) bool {
	// We must always return true, only then will we get the correct 'n' in the 'Partition()' call.
	return true
}

// Partition determines, among a set of n partitions, which index should
// be chosen to use for the partition for r.
func (c *customTopicPartitioner) Partition(r *kgo.Record, n int) int {
	// We expect n to be equal to the partition count of the topic
	// If, for whatever reason, that is false, we print a warning
	if c.expectedPartitionCount != n {
		// todo: maybe this should be an error?
		//       we can probably fix ourselves by just restarting...
		c.logger.Warn("end-to-end TopicPartitioner expected a different number of partitions. This means that kminion has either too many or too few producers to produce to all partitions of the topic.",
			zap.Int("expectedPartitionCount", c.expectedPartitionCount),
			zap.Int("givenPartitionCount", n),
		)
	}

	// If the message wants to be produced to a partitionID higher than what is available, immediately error out by returning -1
	// This should never happen since it would mean that the topics partition count has been changed (!?)
	p := int(r.Partition)
	if p >= n {
		return -1 // partition doesn't exist
	}

	return p
}
