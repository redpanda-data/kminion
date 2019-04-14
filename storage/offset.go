package storage

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	log "github.com/sirupsen/logrus"
	"sync"
)

// PartitionWaterMarks represents a map of PartitionWaterMarks grouped by PartitionID
type PartitionWaterMarks = map[int32]kafka.PartitionWaterMark

// OffsetStorage stores the latest commited offsets for each group, topic, partition combination and offers an interface
// to access these information
type OffsetStorage struct {
	consumerOffsetCh <-chan *kafka.StorageRequest
	clusterCh        <-chan *kafka.StorageRequest

	notReadyPartitionConsumers int32
	offsetTopicConsumed        bool

	consumerStatusLock          sync.RWMutex // Lock is being used if you either access notReadyPartitionConsumers or offsetTopicConsumed
	consumerOffsetsLock         sync.RWMutex
	partitionHighWaterMarksLock sync.RWMutex
	partitionLowWaterMarksLock  sync.RWMutex
	groupMetadataLock           sync.RWMutex
	topicConfigLock             sync.RWMutex

	// consumerOffsets key is "group:topic:partition" (e.g. "sample-consumer:order:20")
	// partitionHighWaterMarks key is "topicname:partitionId" (e.g. "order:20")
	// partitionLowWaterMarks key is "topicname:partitionId" (e.g. "order:20")
	// groupMetadata key is the group id (e.g. "sample-consumer")
	consumerOffsets         map[string]ConsumerPartitionOffsetMetric
	partitionHighWaterMarks map[string]PartitionWaterMarks
	partitionLowWaterMarks  map[string]PartitionWaterMarks
	groupMetadata           map[string]kafka.ConsumerGroupMetadata
	topicConfig             map[string]kafka.TopicConfiguration

	logger *log.Entry
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

// NewOffsetStorage creates a new storage and preinitializes the required maps which store the PartitionOffset information
func NewOffsetStorage(consumerOffsetCh <-chan *kafka.StorageRequest, clusterCh <-chan *kafka.StorageRequest) *OffsetStorage {
	return &OffsetStorage{
		consumerOffsetCh: consumerOffsetCh,
		clusterCh:        clusterCh,

		notReadyPartitionConsumers: 0,
		offsetTopicConsumed:        false,

		consumerOffsets:         make(map[string]ConsumerPartitionOffsetMetric),
		partitionHighWaterMarks: make(map[string]PartitionWaterMarks),
		partitionLowWaterMarks:  make(map[string]PartitionWaterMarks),
		topicConfig:             make(map[string]kafka.TopicConfiguration),
		groupMetadata:           make(map[string]kafka.ConsumerGroupMetadata),

		logger: log.WithFields(log.Fields{
			"module": "storage",
		}),
	}
}

// Start starts listening for incoming offset entries on the input channel so that they can be stored
func (module *OffsetStorage) Start() {
	go module.consumerOffsetWorker()
	go module.clusterWorker()
}

func (module *OffsetStorage) consumerOffsetWorker() {
	for request := range module.consumerOffsetCh {
		switch request.RequestType {
		case kafka.StorageAddConsumerOffset:
			module.storeOffsetEntry(request.ConsumerOffset)
		case kafka.StorageAddGroupMetadata:
			module.storeGroupMetadata(request.GroupMetadata)
		case kafka.StorageDeleteConsumerGroup:
			module.deleteOffsetEntry(request.ConsumerGroupName, request.TopicName, request.PartitionID)
		case kafka.StorageRegisterOffsetPartition:
			module.registerOffsetPartition(request.PartitionID)
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

func (module *OffsetStorage) clusterWorker() {
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

func (module *OffsetStorage) deleteTopic(topicName string) {
	module.topicConfigLock.Lock()
	module.partitionLowWaterMarksLock.Lock()
	module.partitionHighWaterMarksLock.Lock()
	defer module.topicConfigLock.Unlock()
	defer module.partitionHighWaterMarksLock.Unlock()
	defer module.partitionLowWaterMarksLock.Unlock()

	delete(module.partitionLowWaterMarks, topicName)
	delete(module.partitionHighWaterMarks, topicName)
	delete(module.topicConfig, topicName)
}

func (module *OffsetStorage) storePartitionHighWaterMark(offset *kafka.PartitionWaterMark) {
	module.partitionHighWaterMarksLock.Lock()
	defer module.partitionHighWaterMarksLock.Unlock()

	// Initialize entry if needed
	if _, exists := module.partitionHighWaterMarks[offset.TopicName]; !exists {
		module.partitionHighWaterMarks[offset.TopicName] = make(PartitionWaterMarks)
	}

	module.partitionHighWaterMarks[offset.TopicName][offset.PartitionID] = *offset
}

func (module *OffsetStorage) storePartitionLowWaterMark(offset *kafka.PartitionWaterMark) {
	module.partitionLowWaterMarksLock.Lock()
	defer module.partitionLowWaterMarksLock.Unlock()

	// Initialize entry if needed
	if _, exists := module.partitionLowWaterMarks[offset.TopicName]; !exists {
		module.partitionLowWaterMarks[offset.TopicName] = make(PartitionWaterMarks)
	}

	module.partitionLowWaterMarks[offset.TopicName][offset.PartitionID] = *offset
}

func (module *OffsetStorage) storeGroupMetadata(metadata *kafka.ConsumerGroupMetadata) {
	module.groupMetadataLock.Lock()
	defer module.groupMetadataLock.Unlock()

	module.groupMetadata[metadata.Group] = *metadata
}

func (module *OffsetStorage) storeTopicConfig(config *kafka.TopicConfiguration) {
	module.topicConfigLock.Lock()
	defer module.topicConfigLock.Unlock()

	module.topicConfig[config.TopicName] = *config
}

func (module *OffsetStorage) registerOffsetPartition(partitionID int32) {
	module.consumerOffsetsLock.Lock()
	defer module.consumerOffsetsLock.Unlock()

	module.notReadyPartitionConsumers++
}

func (module *OffsetStorage) markOffsetPartitionReady(partitionID int32) {
	module.consumerStatusLock.Lock()
	defer module.consumerStatusLock.Unlock()

	module.notReadyPartitionConsumers--
	if module.notReadyPartitionConsumers == 0 {
		module.logger.Info("Offset topic has been consumed")
		module.offsetTopicConsumed = true
	}
}

func (module *OffsetStorage) storeOffsetEntry(offset *kafka.ConsumerPartitionOffset) {
	key := fmt.Sprintf("%v:%v:%v", offset.Group, offset.Topic, offset.Partition)
	module.consumerOffsetsLock.Lock()
	var commitCount float64
	if entry, exists := module.consumerOffsets[key]; exists {
		commitCount = entry.TotalCommitCount
	}
	commitCount++
	module.consumerOffsets[key] = ConsumerPartitionOffsetMetric{
		Group:            offset.Group,
		Topic:            offset.Topic,
		Partition:        offset.Partition,
		Offset:           offset.Offset,
		Timestamp:        offset.Timestamp,
		TotalCommitCount: commitCount,
	}
	module.consumerOffsetsLock.Unlock()
}

func (module *OffsetStorage) deleteOffsetEntry(consumerGroupName string, topicName string, partitionID int32) {
	key := fmt.Sprintf("%v:%v:%v", consumerGroupName, topicName, partitionID)
	module.consumerOffsetsLock.Lock()
	defer module.consumerOffsetsLock.Unlock()

	delete(module.consumerOffsets, key)
}

// ConsumerOffsets returns a copy of the currently known consumer group offsets, so that they can safely be processed
// in another go routine
func (module *OffsetStorage) ConsumerOffsets() map[string]ConsumerPartitionOffsetMetric {
	module.consumerOffsetsLock.RLock()
	defer module.consumerOffsetsLock.RUnlock()

	mapCopy := make(map[string]ConsumerPartitionOffsetMetric)
	for key, value := range module.consumerOffsets {
		mapCopy[key] = value
	}

	return mapCopy
}

// GroupMetadata returns a copy of the currently known group metadata
func (module *OffsetStorage) GroupMetadata() map[string]kafka.ConsumerGroupMetadata {
	module.groupMetadataLock.RLock()
	defer module.groupMetadataLock.RUnlock()

	mapCopy := make(map[string]kafka.ConsumerGroupMetadata)
	for key, value := range module.groupMetadata {
		mapCopy[key] = value
	}

	return mapCopy
}

// TopicConfigs returns all topic configurations in a copied map, so that it
// is safe to process in another go routine
func (module *OffsetStorage) TopicConfigs() map[string]kafka.TopicConfiguration {
	module.topicConfigLock.RLock()
	defer module.topicConfigLock.RUnlock()

	mapCopy := make(map[string]kafka.TopicConfiguration)
	for key, value := range module.topicConfig {
		mapCopy[key] = value
	}

	return mapCopy
}

// PartitionHighWaterMarks returns all partition high water marks in a copied map, so that it
// is safe to process in another go routine
func (module *OffsetStorage) PartitionHighWaterMarks() map[string]PartitionWaterMarks {
	module.partitionHighWaterMarksLock.RLock()
	defer module.partitionHighWaterMarksLock.RUnlock()

	mapCopy := make(map[string]PartitionWaterMarks)
	for key, value := range module.partitionHighWaterMarks {
		mapCopy[key] = value
	}

	return mapCopy
}

// PartitionLowWaterMarks returns all partition low water marks in a copied map, so that it
// is safe to process in another go routine
func (module *OffsetStorage) PartitionLowWaterMarks() map[string]PartitionWaterMarks {
	module.partitionLowWaterMarksLock.RLock()
	defer module.partitionLowWaterMarksLock.RUnlock()

	mapCopy := make(map[string]PartitionWaterMarks)
	for key, value := range module.partitionLowWaterMarks {
		mapCopy[key] = value
	}

	return mapCopy
}

// IsConsumed indicates whether the consumer offsets topic lag has been caught up and therefore
// the metrics reported by this module are accurate or not
func (module *OffsetStorage) IsConsumed() bool {
	module.consumerStatusLock.RLock()
	defer module.consumerStatusLock.RUnlock()

	return module.offsetTopicConsumed
}
