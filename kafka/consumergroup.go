package kafka

// ConsumerGroupTopicLag has details about the topic lag for a single consumer group
type ConsumerGroupTopicLag struct {
	Name       string
	TopicName  string
	TopicLag   int64
	Partitions []*ConsumerGroupPartition
}

// ConsumerGroupPartition contains all lag relevant consumer group information
type ConsumerGroupPartition struct {
	PartitionID    int32
	ConsumerOffset int64
	HighWaterMark  int64
	Lag            int64
}
