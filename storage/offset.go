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
	consumerOffsetCh      chan *kafka.ConsumerPartitionOffset
	partitionWaterMarksCh chan *kafka.PartitionWaterMarks

	// partitionWaterMarks key is "topicname:partitionId" (e.g. "order:20")
	// consumerOffsets key is "group:topic:partition" (e.g. "sample-consumer:order:20")
	consumerOffsetsLock     sync.RWMutex
	consumerOffsets         map[string]kafka.ConsumerPartitionOffset
	partitionWaterMarksLock sync.RWMutex
	partitionWaterMarks     map[string]kafka.PartitionWaterMarks
}

// NewOffsetStorage creates a new storage and preinitializes the required maps which store the PartitionOffset information
func NewOffsetStorage(consumerOffsetCh chan *kafka.ConsumerPartitionOffset, partitionWaterMarksCh chan *kafka.PartitionWaterMarks) *OffsetStorage {
	return &OffsetStorage{
		consumerOffsetCh:      consumerOffsetCh,
		partitionWaterMarksCh: partitionWaterMarksCh,
		consumerOffsetsLock:   sync.RWMutex{},
		consumerOffsets:       make(map[string]kafka.ConsumerPartitionOffset),
		partitionWaterMarks:   make(map[string]kafka.PartitionWaterMarks),
	}
}

// Start starts listening for incoming offset entries on the input channel so that they can be stored
func (module *OffsetStorage) Start() {
	go module.consumerOffsetStorageWorker()
	go module.partitionWaterMarksWorker()
}

func (module *OffsetStorage) consumerOffsetStorageWorker() {
	for consumerOffset := range module.consumerOffsetCh {
		module.storeOffsetEntry(consumerOffset)
	}
	log.Panic("Group offset storage channel closed")
}

func (module *OffsetStorage) partitionWaterMarksWorker() {
	for partitionOffset := range module.partitionWaterMarksCh {
		module.storePartitionOffsetEntry(partitionOffset)
	}
	log.Panic("Partition Offset storage channel closed")
}

func (module *OffsetStorage) storePartitionOffsetEntry(offset *kafka.PartitionWaterMarks) {
	key := fmt.Sprintf("%v:%v", offset.TopicName, offset.PartitionID)
	module.partitionWaterMarksLock.Lock()
	module.partitionWaterMarks[key] = *offset
	module.partitionWaterMarksLock.Unlock()
}

func (module *OffsetStorage) storeOffsetEntry(offset *kafka.ConsumerPartitionOffset) {
	key := fmt.Sprintf("%v:%v:%v", offset.Group, offset.Topic, offset.Partition)
	module.consumerOffsetsLock.Lock()
	module.consumerOffsets[key] = *offset
	module.consumerOffsetsLock.Unlock()
}

// TODO remove outdated data
func (module *OffsetStorage) ConsumerOffsets() map[string]kafka.ConsumerPartitionOffset {
	module.consumerOffsetsLock.RLock()
	defer module.consumerOffsetsLock.RUnlock()

	return module.consumerOffsets
}

// TODO emove outdated data
func (module *OffsetStorage) PartitionWaterMarks() map[string]kafka.PartitionWaterMarks {
	module.partitionWaterMarksLock.RLock()
	defer module.partitionWaterMarksLock.RUnlock()

	return module.partitionWaterMarks
}
