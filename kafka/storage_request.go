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
)

// StorageRequest is an entity to send requests to the storage module
type StorageRequest struct {
	RequestType            StorageRequestType
	ConsumerOffset         *ConsumerPartitionOffset
	PartitionHighWaterMark *PartitionHighWaterMark
}

func newAddPartitionHighWaterMarkRequest(highWaterMark *PartitionHighWaterMark) *StorageRequest {
	return &StorageRequest{
		RequestType:            StorageAddPartitionHighWaterMark,
		PartitionHighWaterMark: highWaterMark,
	}
}

func newAddConsumerOffsetRequest(offset *ConsumerPartitionOffset) *StorageRequest {
	return &StorageRequest{
		RequestType:    StorageAddConsumerOffset,
		ConsumerOffset: offset,
	}
}
