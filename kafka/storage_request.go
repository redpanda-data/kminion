package kafka

// StorageRequestType is used to determine the message type / request when communicating with the
// storage module via channel. Depending on the request type you must provide additional information
// so that the request can be processed.
type StorageRequestType int

const (
	// StorageAddPartitionHighWaterMark is the request type to add a partition's high water mark
	StorageAddPartitionHighWaterMark StorageRequestType = 0

	// StorageAddPartitionLowWaterMark is the request type to add a partition's low water mark
	StorageAddPartitionLowWaterMark StorageRequestType = 1

	// StorageAddConsumerOffset is the request type to add a consumer's offset commit
	StorageAddConsumerOffset StorageRequestType = 2

	// StorageAddGroupMetadata is the request type to add a group member's partition assignment
	StorageAddGroupMetadata StorageRequestType = 3

	// StorageAddTopicConfiguration is the request type to add configuration entries for a topic
	StorageAddTopicConfiguration StorageRequestType = 4

	// StorageDeleteConsumerGroup is the request type to remove an offset commit for a topic:group:partition combination
	StorageDeleteConsumerGroup StorageRequestType = 5

	// StorageRegisterOffsetPartitions is the request type to make the storage module aware that the offset consumer
	// first has to fully consume a specific number of partitions before it should expose any metrics
	StorageRegisterOffsetPartitions StorageRequestType = 6

	// StorageMarkOffsetPartitionReady is the request type to mark a partition consumer of the consumer offsets topic
	// as ready (=caught up partition lag)
	StorageMarkOffsetPartitionReady StorageRequestType = 7

	// StorageDeleteGroupMetadata is the request type to delete a group member's partition assignment
	StorageDeleteGroupMetadata StorageRequestType = 8

	// StorageDeleteTopic is the request type to delete all topic information
	StorageDeleteTopic StorageRequestType = 9
)

// StorageRequest is an entity to send messages / requests to the storage module.
type StorageRequest struct {
	RequestType        StorageRequestType
	ConsumerOffset     *ConsumerPartitionOffset
	PartitionWaterMark *PartitionWaterMark
	TopicConfig        *TopicConfiguration
	GroupMetadata      *ConsumerGroupMetadata
	ConsumerGroupName  string
	TopicName          string
	PartitionID        int32
	PartitionCount     int
}

func newAddPartitionLowWaterMarkRequest(lowWaterMark *PartitionWaterMark) *StorageRequest {
	return &StorageRequest{
		RequestType:        StorageAddPartitionLowWaterMark,
		PartitionWaterMark: lowWaterMark,
	}
}

func newAddPartitionHighWaterMarkRequest(highWaterMark *PartitionWaterMark) *StorageRequest {
	return &StorageRequest{
		RequestType:        StorageAddPartitionHighWaterMark,
		PartitionWaterMark: highWaterMark,
	}
}

func newAddConsumerOffsetRequest(offset *ConsumerPartitionOffset) *StorageRequest {
	return &StorageRequest{
		RequestType:    StorageAddConsumerOffset,
		ConsumerOffset: offset,
	}
}

func newAddGroupMetadata(metadata *ConsumerGroupMetadata) *StorageRequest {
	return &StorageRequest{
		RequestType:   StorageAddGroupMetadata,
		GroupMetadata: metadata,
	}
}

func newAddTopicConfig(config *TopicConfiguration) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageAddTopicConfiguration,
		TopicConfig: config,
	}
}

func newDeleteConsumerGroupRequest(group string, topic string, partitionID int32) *StorageRequest {
	return &StorageRequest{
		RequestType:       StorageDeleteConsumerGroup,
		ConsumerGroupName: group,
		TopicName:         topic,
		PartitionID:       partitionID,
	}
}

func newRegisterOffsetPartitionsRequest(partitionCount int) *StorageRequest {
	return &StorageRequest{
		RequestType:    StorageRegisterOffsetPartitions,
		PartitionCount: partitionCount,
	}
}

func newMarkOffsetPartitionReadyRequest(partitionID int32) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageMarkOffsetPartitionReady,
		PartitionID: partitionID,
	}
}

func newDeleteGroupMetadataRequest(group string) *StorageRequest {
	return &StorageRequest{
		RequestType:       StorageDeleteGroupMetadata,
		ConsumerGroupName: group,
	}
}

func newDeleteTopicRequest(topic string) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageDeleteTopic,
		TopicName:   topic,
	}
}
