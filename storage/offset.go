package storage

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	log "github.com/sirupsen/logrus"
	"sync"
)

// OffsetStorage stores the latest commited offsets for each group, topic, partition combination and offers an interface
// to access these information
type OffsetStorage struct {
	consumerOffsetCh chan *kafka.StorageRequest
	clusterCh        chan *kafka.StorageRequest

	consumerOffsetsLock         sync.RWMutex
	partitionHighWaterMarksLock sync.RWMutex
	partitionLowWaterMarksLock  sync.RWMutex

	// consumerOffsets key is "group:topic:partition" (e.g. "sample-consumer:order:20")
	// partitionHighWaterMarks key is "topicname:partitionId" (e.g. "order:20")
	consumerOffsets         map[string]ConsumerPartitionOffsetMetric
	partitionHighWaterMarks map[string]kafka.PartitionWaterMark
	partitionLowWaterMarks  map[string]kafka.PartitionWaterMark
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
func NewOffsetStorage(consumerOffsetCh chan *kafka.StorageRequest, clusterCh chan *kafka.StorageRequest) *OffsetStorage {
	return &OffsetStorage{
		consumerOffsetCh: consumerOffsetCh,
		clusterCh:        clusterCh,

		consumerOffsetsLock:         sync.RWMutex{},
		partitionHighWaterMarksLock: sync.RWMutex{},
		partitionLowWaterMarksLock:  sync.RWMutex{},

		consumerOffsets:         make(map[string]ConsumerPartitionOffsetMetric),
		partitionHighWaterMarks: make(map[string]kafka.PartitionWaterMark),
		partitionLowWaterMarks:  make(map[string]kafka.PartitionWaterMark),
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
		case kafka.StorageDeleteConsumerGroup:
			module.deleteOffsetEntry(request.ConsumerGroupName, request.TopicName, request.PartitionID)
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
		default:
			log.WithFields(log.Fields{
				"request_type": request.RequestType,
				"channel":      "clusterWorkerCh",
			}).Error("unknown request type")
		}
	}
	log.Panic("Partition Offset storage channel closed")
}

func (module *OffsetStorage) storePartitionHighWaterMark(offset *kafka.PartitionWaterMark) {
	key := fmt.Sprintf("%v:%v", offset.TopicName, offset.PartitionID)
	module.partitionHighWaterMarksLock.Lock()
	module.partitionHighWaterMarks[key] = *offset
	module.partitionHighWaterMarksLock.Unlock()
}

func (module *OffsetStorage) storePartitionLowWaterMark(offset *kafka.PartitionWaterMark) {
	key := fmt.Sprintf("%v:%v", offset.TopicName, offset.PartitionID)
	module.partitionLowWaterMarksLock.Lock()
	module.partitionLowWaterMarks[key] = *offset
	module.partitionLowWaterMarksLock.Unlock()
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
	delete(module.consumerOffsets, key)
	module.consumerOffsetsLock.Unlock()
}

// ConsumerOffsets returns a copy of the current known consumer group offsets, so that they can safely be processed
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

// TODO remove outdated data
func (module *OffsetStorage) PartitionHighWaterMarks() map[string]kafka.PartitionWaterMark {
	module.partitionHighWaterMarksLock.RLock()
	defer module.partitionHighWaterMarksLock.RUnlock()

	mapCopy := make(map[string]kafka.PartitionWaterMark)
	for key, value := range module.partitionHighWaterMarks {
		mapCopy[key] = value
	}

	return mapCopy
}

// TODO remove outdated data
func (module *OffsetStorage) PartitionLowWaterMarks() map[string]kafka.PartitionWaterMark {
	module.partitionLowWaterMarksLock.RLock()
	defer module.partitionLowWaterMarksLock.RUnlock()

	mapCopy := make(map[string]kafka.PartitionWaterMark)
	for key, value := range module.partitionLowWaterMarks {
		mapCopy[key] = value
	}

	return mapCopy
}
