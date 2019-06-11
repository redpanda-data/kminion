package storage

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
)

// PartitionWaterMarks represents a map of PartitionWaterMarks grouped by PartitionID
type PartitionWaterMarks = map[int32]kafka.PartitionWaterMark

// MemoryStorage stores the latest committed offsets for each group, topic, partition combination and offers an interface
// to access these information
type MemoryStorage struct {
	logger *log.Entry

	// Channels for receiving storage requests
	consumerOffsetCh <-chan *kafka.StorageRequest
	clusterCh        <-chan *kafka.StorageRequest

	status     *consumerStatus
	groups     *consumerGroup
	partitions *partition
	topics     *topic
}

// consumerStatus holds information about the partition consumers consuming the __consumer_offsets topic
type consumerStatus struct {
	Lock                       sync.RWMutex
	NotReadyPartitionConsumers int
	OffsetTopicConsumed        bool
}

// consumerGroup contains all consumer group data such as offsets or metadata
type consumerGroup struct {
	OffsetsLock sync.RWMutex
	Offsets     map[string]ConsumerPartitionOffsetMetric

	MetadataLock sync.RWMutex
	Metadata     map[string]kafka.ConsumerGroupMetadata
}

type partition struct {
	LowWaterMarksLock sync.RWMutex
	LowWaterMarks     map[string]PartitionWaterMarks

	HighWaterMarksLock sync.RWMutex
	HighWaterMarks     map[string]PartitionWaterMarks
}

type topic struct {
	ConfigsLock sync.RWMutex
	Configs     map[string]kafka.TopicConfiguration
}

// ConsumerPartitionOffsetMetric represents an offset commit but is extended by further fields which can be
// substituted using the given information (e. g. TotalCommitCount to calculate the commit rate)
type ConsumerPartitionOffsetMetric struct {
	Group            string
	Topic            string
	Partition        int32
	Offset           int64
	Timestamp        int64
	TotalCommitCount float64
}

// NewMemoryStorage creates a new storage and preinitializes the required maps which store the PartitionOffset information
func NewMemoryStorage(consumerOffsetCh <-chan *kafka.StorageRequest, clusterCh <-chan *kafka.StorageRequest) *MemoryStorage {
	groups := &consumerGroup{
		Offsets:  make(map[string]ConsumerPartitionOffsetMetric),
		Metadata: make(map[string]kafka.ConsumerGroupMetadata),
	}

	status := &consumerStatus{
		NotReadyPartitionConsumers: math.MaxInt32,
		OffsetTopicConsumed:        false,
	}

	partitions := &partition{
		LowWaterMarks:  make(map[string]PartitionWaterMarks),
		HighWaterMarks: make(map[string]PartitionWaterMarks),
	}

	topics := &topic{
		Configs: make(map[string]kafka.TopicConfiguration),
	}

	return &MemoryStorage{
		logger: log.WithFields(log.Fields{
			"module": "storage",
		}),

		consumerOffsetCh: consumerOffsetCh,
		clusterCh:        clusterCh,

		status:     status,
		groups:     groups,
		partitions: partitions,
		topics:     topics,
	}
}

// Start starts listening for incoming offset entries on the input channel so that they can be stored
func (module *MemoryStorage) Start() {
	go module.consumerOffsetWorker()
	go module.clusterWorker()
}

func (module *MemoryStorage) consumerOffsetWorker() {
	for request := range module.consumerOffsetCh {
		switch request.RequestType {
		case kafka.StorageAddConsumerOffset:
			module.storeOffsetEntry(request.ConsumerOffset)
		case kafka.StorageAddGroupMetadata:
			module.storeGroupMetadata(request.GroupMetadata)
		case kafka.StorageDeleteConsumerGroup:
			module.deleteOffsetEntry(request.ConsumerGroupName, request.TopicName, request.PartitionID)
		case kafka.StorageRegisterOffsetPartitions:
			module.registerOffsetPartitions(request.PartitionCount)
		case kafka.StorageMarkOffsetPartitionReady:
			module.markOffsetPartitionReady(request.PartitionID)

		default:
			log.WithFields(log.Fields{
				"request_type": request.RequestType,
				"channel":      "consumerOffsetsCh",
			}).Error("unknown request type")
		}
	}
	log.Panic("Group offset storage channel closed")
}

func (module *MemoryStorage) clusterWorker() {
	for request := range module.clusterCh {
		switch request.RequestType {
		case kafka.StorageAddPartitionLowWaterMark:
			module.storePartitionLowWaterMark(request.PartitionWaterMark)
		case kafka.StorageAddPartitionHighWaterMark:
			module.storePartitionHighWaterMark(request.PartitionWaterMark)
		case kafka.StorageAddTopicConfiguration:
			module.storeTopicConfig(request.TopicConfig)
		case kafka.StorageDeleteTopic:
			module.deleteTopic(request.TopicName)

		default:
			log.WithFields(log.Fields{
				"request_type": request.RequestType,
				"channel":      "clusterWorkerCh",
			}).Error("unknown request type")
		}
	}
	log.Panic("Partition Offset storage channel closed")
}

func (module *MemoryStorage) deleteTopic(topicName string) {
	module.topics.ConfigsLock.Lock()
	module.partitions.LowWaterMarksLock.Lock()
	module.partitions.HighWaterMarksLock.Lock()
	defer module.topics.ConfigsLock.Unlock()
	defer module.partitions.LowWaterMarksLock.Unlock()
	defer module.partitions.HighWaterMarksLock.Unlock()

	delete(module.partitions.LowWaterMarks, topicName)
	delete(module.partitions.HighWaterMarks, topicName)
	delete(module.topics.Configs, topicName)
}

func (module *MemoryStorage) storePartitionHighWaterMark(offset *kafka.PartitionWaterMark) {
	module.partitions.HighWaterMarksLock.Lock()
	defer module.partitions.HighWaterMarksLock.Unlock()

	// Initialize entry if needed
	if _, exists := module.partitions.HighWaterMarks[offset.TopicName]; !exists {
		module.partitions.HighWaterMarks[offset.TopicName] = make(PartitionWaterMarks)
	}

	module.partitions.HighWaterMarks[offset.TopicName][offset.PartitionID] = *offset
}

func (module *MemoryStorage) storePartitionLowWaterMark(offset *kafka.PartitionWaterMark) {
	module.partitions.LowWaterMarksLock.Lock()
	defer module.partitions.LowWaterMarksLock.Unlock()

	// Initialize entry if needed
	if _, exists := module.partitions.LowWaterMarks[offset.TopicName]; !exists {
		module.partitions.LowWaterMarks[offset.TopicName] = make(PartitionWaterMarks)
	}

	module.partitions.LowWaterMarks[offset.TopicName][offset.PartitionID] = *offset
}

func (module *MemoryStorage) storeGroupMetadata(metadata *kafka.ConsumerGroupMetadata) {
	module.groups.MetadataLock.Lock()
	defer module.groups.MetadataLock.Unlock()

	module.groups.Metadata[metadata.Group] = *metadata
}

func (module *MemoryStorage) storeTopicConfig(config *kafka.TopicConfiguration) {
	module.topics.ConfigsLock.Lock()
	defer module.topics.ConfigsLock.Unlock()

	module.topics.Configs[config.TopicName] = *config
}

func (module *MemoryStorage) registerOffsetPartitions(partitionCount int) {
	module.status.Lock.Lock()
	defer module.status.Lock.Unlock()

	module.logger.Infof("Registered %v __consumer_offsets partitions which have to be consumed before metrics can be exposed", partitionCount)
	module.status.NotReadyPartitionConsumers = partitionCount
}

func (module *MemoryStorage) markOffsetPartitionReady(partitionID int32) {
	module.status.Lock.Lock()
	defer module.status.Lock.Unlock()

	module.status.NotReadyPartitionConsumers--
	if module.status.NotReadyPartitionConsumers == 0 {
		module.logger.Info("Offset topic has been consumed")
		module.status.OffsetTopicConsumed = true
	}
}

func (module *MemoryStorage) storeOffsetEntry(offset *kafka.ConsumerPartitionOffset) {
	module.groups.OffsetsLock.Lock()
	defer module.groups.OffsetsLock.Unlock()

	key := fmt.Sprintf("%v:%v:%v", offset.Group, offset.Topic, offset.Partition)
	var commitCount float64
	if entry, exists := module.groups.Offsets[key]; exists {
		commitCount = entry.TotalCommitCount
	}
	commitCount++
	module.groups.Offsets[key] = ConsumerPartitionOffsetMetric{
		Group:            offset.Group,
		Topic:            offset.Topic,
		Partition:        offset.Partition,
		Offset:           offset.Offset,
		Timestamp:        offset.Timestamp,
		TotalCommitCount: commitCount,
	}
}

func (module *MemoryStorage) deleteOffsetEntry(consumerGroupName string, topicName string, partitionID int32) {
	key := fmt.Sprintf("%v:%v:%v", consumerGroupName, topicName, partitionID)
	module.groups.OffsetsLock.Lock()
	defer module.groups.OffsetsLock.Unlock()

	delete(module.groups.Offsets, key)
}

// ConsumerOffsets returns a copy of the currently known consumer group offsets, so that they can safely be processed
// in another go routine
func (module *MemoryStorage) ConsumerOffsets() map[string]ConsumerPartitionOffsetMetric {
	module.groups.OffsetsLock.RLock()
	defer module.groups.OffsetsLock.RUnlock()

	mapCopy := make(map[string]ConsumerPartitionOffsetMetric)
	for key, value := range module.groups.Offsets {
		mapCopy[key] = value
	}

	return mapCopy
}

// GroupMetadata returns a copy of the currently known group metadata
func (module *MemoryStorage) GroupMetadata() map[string]kafka.ConsumerGroupMetadata {
	module.groups.MetadataLock.RLock()
	defer module.groups.MetadataLock.RUnlock()

	mapCopy := make(map[string]kafka.ConsumerGroupMetadata)
	for key, value := range module.groups.Metadata {
		mapCopy[key] = value
	}

	return mapCopy
}

// TopicConfigs returns all topic configurations in a copied map, so that it
// is safe to process in another go routine
func (module *MemoryStorage) TopicConfigs() map[string]kafka.TopicConfiguration {
	module.topics.ConfigsLock.RLock()
	defer module.topics.ConfigsLock.RUnlock()

	mapCopy := make(map[string]kafka.TopicConfiguration)
	for key, value := range module.topics.Configs {
		mapCopy[key] = value
	}

	return mapCopy
}

// PartitionHighWaterMarks returns all partition high water marks in a copied map, so that it
// is safe to process in another go routine
func (module *MemoryStorage) PartitionHighWaterMarks() map[string]PartitionWaterMarks {
	module.partitions.HighWaterMarksLock.RLock()
	defer module.partitions.HighWaterMarksLock.RUnlock()

	mapCopy := make(map[string]PartitionWaterMarks)
	for key, value := range module.partitions.HighWaterMarks {
		mapCopy[key] = make(PartitionWaterMarks)
		for partition, partitionData := range value {
			mapCopy[key][partition] = partitionData
		}
	}

	return mapCopy
}

// PartitionLowWaterMarks returns all partition low water marks in a copied map, so that it
// is safe to process in another go routine
func (module *MemoryStorage) PartitionLowWaterMarks() map[string]PartitionWaterMarks {
	module.partitions.LowWaterMarksLock.RLock()
	defer module.partitions.LowWaterMarksLock.RUnlock()

	mapCopy := make(map[string]PartitionWaterMarks)
	for key, value := range module.partitions.LowWaterMarks {
		mapCopy[key] = make(PartitionWaterMarks)
		for partition, partitionData := range value {
			mapCopy[key][partition] = partitionData
		}
	}

	return mapCopy
}

// IsConsumed indicates whether the consumer offsets topic lag has been caught up and therefore
// the metrics reported by this module are accurate or not
func (module *MemoryStorage) IsConsumed() bool {
	module.status.Lock.RLock()
	defer module.status.Lock.RUnlock()

	return module.status.OffsetTopicConsumed
}
