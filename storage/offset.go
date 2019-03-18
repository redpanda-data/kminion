package storage

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	log "github.com/sirupsen/logrus"
)

// OffsetStorage stores the latest commited offsets for each group, topic, partition combination and offers an interface
// to access these information
type OffsetStorage struct {
	inputOffsetChannel chan *kafka.OffsetEntry
	offsets            map[string]*PartitionOffset
}

// PartitionOffset describes an offset commit entry by a Consumer Group
type PartitionOffset struct {
	Group               string
	Topic               string
	Partition           int32
	Offset              int64
	LastCommitTimestamp int64
}

// NewOffsetStorage creates a new storage and preinitializes the required maps which store the PartitionOffset information
func NewOffsetStorage(storageChannel chan *kafka.OffsetEntry) *OffsetStorage {
	return &OffsetStorage{
		inputOffsetChannel: storageChannel,
		offsets:            make(map[string]*PartitionOffset),
	}
}

// Start starts listening for incoming offset entries on the input channel so that they can be stored
func (o *OffsetStorage) Start() {
	go func() {
		for offset := range o.inputOffsetChannel {
			o.storeOffsetEntry(offset)
		}
		log.Error("Storage channel closed")
	}()
}

func (o *OffsetStorage) storeOffsetEntry(offset *kafka.OffsetEntry) {
	log.Debugf("Group %v - Topic: %v - Partition: %v - Offset: %v", offset.Group, offset.Topic, offset.Partition, offset.Offset)

	key := fmt.Sprintf("%v-%v-%v", offset.Group, offset.Topic, offset.Partition)
	o.offsets[key] = &PartitionOffset{
		Group:               offset.Group,
		Topic:               offset.Topic,
		Partition:           offset.Partition,
		Offset:              offset.Offset,
		LastCommitTimestamp: offset.Timestamp,
	}
}

func (o *OffsetStorage) Offsets() map[string]*PartitionOffset {
	return o.offsets
}
