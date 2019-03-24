package kafka

// StorageRequestType is used in StorageRequest to indicate the type of request.
type StorageRequestType int

const (
	// StorageAddPartitionHighWaterMark is the request type to add a partition's high water mark
	StorageAddPartitionHighWaterMark StorageRequestType = 0

	// StorageAddPartitionLowWaterMark is the request type to add a partition's low water mark
	StorageAddPartitionLowWaterMark StorageRequestType = 1

	// StorageAddConsumerOffset is the request type to add a consumer's offset commit
	StorageAddConsumerOffset StorageRequestType = 2

	// StorageDeleteConsumerGroup is the request type to remove an offset commit for a topic:group:partition combination
	StorageDeleteConsumerGroup StorageRequestType = 3
)

// StorageRequest is an entity to send requests to the storage module
type StorageRequest struct {
	RequestType        StorageRequestType
	ConsumerOffset     *ConsumerPartitionOffset
	PartitionWaterMark *PartitionWaterMark
	ConsumerGroupName  string
	TopicName          string
	PartitionID        int32
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

func newDeleteConsumerGroupRequest(group string, topic string, partitionID int32) *StorageRequest {
	return &StorageRequest{
		RequestType:       StorageDeleteConsumerGroup,
		ConsumerGroupName: group,
		TopicName:         topic,
		PartitionID:       partitionID,
	}
}
