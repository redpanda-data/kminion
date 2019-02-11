package kafka

// ConsumerGroupTopicLag has details about the topic lag for a single consumer group
type ConsumerGroupTopicLag struct {
	Name      string
	TopicName string
	TopicLag  int64
}
