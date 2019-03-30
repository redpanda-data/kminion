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

	// StorageDeleteConsumerGroup is the request type to remove an offset commit for a topic:group:partition combination
	StorageDeleteConsumerGroup StorageRequestType = 3

	// StorageRegisterOffsetPartition is the request type to make the storage module aware that a partition consumer for
	// the consumer offsets partition exists and that it should await it's ready signal before exposing metrics
	StorageRegisterOffsetPartition StorageRequestType = 4

	// StorageMarkOffsetPartitionReady is the request type to mark a partition consumer of the consumer offsets topic
	// as ready (=caught up partition lag)
	StorageMarkOffsetPartitionReady StorageRequestType = 5
)

// StorageRequest is an entity to send messages / requests to the storage module.
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

func newRegisterOffsetPartition(partitionID int32) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageRegisterOffsetPartition,
		PartitionID: partitionID,
	}
}

func newMarkOffsetPartitionReady(partitionID int32) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageMarkOffsetPartitionReady,
		PartitionID: partitionID,
	}
}
