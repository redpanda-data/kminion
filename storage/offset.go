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

	// partitionWaterMarks key is "topicname:partitionId" (e.g. "order:20")
	// consumerOffsets key is "group:topic:partition" (e.g. "sample-consumer:order:20")
	consumerOffsetsLock         sync.RWMutex
	consumerOffsets             map[string]kafka.ConsumerPartitionOffset
	partitionHighWaterMarksLock sync.RWMutex
	partitionHighWaterMarks     map[string]kafka.PartitionHighWaterMark
}

// NewOffsetStorage creates a new storage and preinitializes the required maps which store the PartitionOffset information
func NewOffsetStorage(consumerOffsetCh chan *kafka.StorageRequest, clusterCh chan *kafka.StorageRequest) *OffsetStorage {
	return &OffsetStorage{
		consumerOffsetCh:        consumerOffsetCh,
		clusterCh:               clusterCh,
		consumerOffsetsLock:     sync.RWMutex{},
		consumerOffsets:         make(map[string]kafka.ConsumerPartitionOffset),
		partitionHighWaterMarks: make(map[string]kafka.PartitionHighWaterMark),
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
		case kafka.StorageAddPartitionHighWaterMark:
			module.storePartitionOffsetEntry(request.PartitionHighWaterMark)
		}
	}
	log.Panic("Partition Offset storage channel closed")
}

func (module *OffsetStorage) storePartitionOffsetEntry(offset *kafka.PartitionHighWaterMark) {
	key := fmt.Sprintf("%v:%v", offset.TopicName, offset.PartitionID)
	module.partitionHighWaterMarksLock.Lock()
	module.partitionHighWaterMarks[key] = *offset
	module.partitionHighWaterMarksLock.Unlock()
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
func (module *OffsetStorage) PartitionWaterMarks() map[string]kafka.PartitionHighWaterMark {
	module.partitionHighWaterMarksLock.RLock()
	defer module.partitionHighWaterMarksLock.RUnlock()

	return module.partitionHighWaterMarks
}
