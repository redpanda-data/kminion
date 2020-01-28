package kafka

// StorageRequestType is used to determine the message type / request when communicating with the
// storage module via channel. Depending on the request type you must provide additional information
// so that the request can be processed.
type StorageRequestType int

const (
	// StorageAddPartitionHighWaterMark is the request type to add a partition's high water mark
	StorageAddPartitionHighWaterMark StorageRequestType = iota + 1

	// StorageAddPartitionLowWaterMark is the request type to add a partition's low water mark
	StorageAddPartitionLowWaterMark

	// StorageAddConsumerOffset is the request type to add a consumer's offset commit
	StorageAddConsumerOffset

	// StorageAddGroupMetadata is the request type to add a group member's partition assignment
	StorageAddGroupMetadata

	// StorageAddTopicConfiguration is the request type to add configuration entries for a topic
	StorageAddTopicConfiguration

	// StorageAddSizeByTopic is the request type to add aggregated partition sizes grouped by topic
	StorageAddSizeByTopic

	// StorageAddSizeByBroker is the request type to add aggregated partition sizes grouped by broker
	StorageAddSizeByBroker

	// StorageDeleteConsumerGroup is the request type to remove an offset commit for a topic:group:partition combination
	StorageDeleteConsumerGroup

	// StorageRegisterOffsetPartitions is the request type to make the storage module aware that the offset consumer
	// first has to fully consume a specific number of partitions before it should expose any metrics
	StorageRegisterOffsetPartitions

	// StorageMarkOffsetPartitionReady is the request type to mark a partition consumer of the consumer offsets topic
	// as ready (=caught up partition lag)
	StorageMarkOffsetPartitionReady

	// StorageDeleteGroupMetadata is the request type to delete a group member's partition assignment
	StorageDeleteGroupMetadata

	// StorageDeleteTopic is the request type to delete all topic information
	StorageDeleteTopic

	// StorageReplicationStatus is the request type to store the current replication status
	StorageReplicationStatus

	// StorageBrokerCount is the request type to store the current number of connected brokers
	StorageBrokerCount
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
	BrokerCount        int
	ReplicationStatus  bool
	SizeByTopic        map[string]int64
	SizeByBroker       map[int32]int64
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

func newReplicationStatusRequest(topic string, partitionID int32, status bool) *StorageRequest {
	return &StorageRequest{
		RequestType:       StorageReplicationStatus,
		TopicName:         topic,
		ReplicationStatus: status,
		PartitionID:       partitionID,
	}
}

func newBrokerCountRequest(number int) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageBrokerCount,
		BrokerCount: number,
	}
}

func newAddSizeByTopicRequest(sizeByTopic map[string]int64) *StorageRequest {
	return &StorageRequest{
		RequestType: StorageAddSizeByTopic,
		SizeByTopic: sizeByTopic,
	}
}

func newAddSizeByBrokerRequest(sizeByBroker map[int32]int64) *StorageRequest {
	return &StorageRequest{
		RequestType:  StorageAddSizeByBroker,
		SizeByBroker: sizeByBroker,
	}
}
