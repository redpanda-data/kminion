package kafka

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"math"
	"strings"
	"sync"
	"time"
)

type consumerStatus struct {
	PartitionLag int64
	IsReady      bool // Indicates whether a partition consumer has caught up the partition lag or not
}

// OffsetConsumer is a consumer module which reads consumer group information from the offsets topic in a Kafka cluster.
// The offsets topic is typically named __consumer_offsets. All messages in this topic are binary and therefore they
// must first be decoded to access the information. This module consumes and processes all messages in the offsets topic.
type OffsetConsumer struct {
	// Waitgroup for all partitionConsumers. For each partition consumer waitgroup is incremented
	wg sync.WaitGroup

	// StorageChannel is used to persist processed messages in memory so that they can be exposed with prometheus
	storageChannel chan<- *StorageRequest

	logger           *log.Entry
	client           sarama.Client
	offsetsTopicName string
	options          *options.Options
}

// NewOffsetConsumer creates a consumer which process all messages in the __consumer_offsets topic
// If it cannot connect to the cluster it will panic
func NewOffsetConsumer(opts *options.Options, storageChannel chan<- *StorageRequest) *OffsetConsumer {
	logger := log.WithFields(log.Fields{
		"module": "offset_consumer",
	})

	// Connect client to at least one of the brokers and verify the connection by requesting metadata
	connectionLogger := logger.WithFields(log.Fields{
		"address": strings.Join(opts.KafkaBrokers, ","),
	})
	clientConfig := saramaClientConfig(opts)
	connectionLogger.Info("Connecting to kafka cluster")
	client, err := sarama.NewClient(opts.KafkaBrokers, clientConfig)
	if err != nil {
		connectionLogger.WithFields(log.Fields{
			"reason": err,
		}).Panicf("failed to start client")
	}
	connectionLogger.Info("Successfully connected to kafka cluster")

	return &OffsetConsumer{
		wg:               sync.WaitGroup{},
		storageChannel:   storageChannel,
		logger:           logger,
		client:           client,
		offsetsTopicName: opts.ConsumerOffsetsTopicName,
		options:          opts,
	}
}

// Start creates partition consumer for each partition in that topic and starts consuming them
func (module *OffsetConsumer) Start() {
	// Create the consumer from the client
	consumer, err := sarama.NewConsumerFromClient(module.client)
	if err != nil {
		log.Panic("failed to get new consumer", err)
	}

	// Get the partition count for the offsets topic
	partitions, err := module.client.Partitions(module.offsetsTopicName)
	if err != nil {
		log.WithFields(log.Fields{
			"topic": module.offsetsTopicName,
			"error": err.Error(),
		}).Panic("failed to get partition count")
	}

	// Start consumers for each partition with fan in
	log.WithFields(log.Fields{
		"topic": module.offsetsTopicName,
		"count": len(partitions),
	}).Infof("Starting '%d' partition consumers", len(partitions))
	registerPartitionRequest := newRegisterOffsetPartitionsRequest(len(partitions))
	module.storageChannel <- registerPartitionRequest
	for _, partition := range partitions {
		module.wg.Add(1)
		go module.partitionConsumer(consumer, partition)
	}
	log.WithFields(log.Fields{
		"topic": module.offsetsTopicName,
		"count": len(partitions),
	}).Info("Spawned all consumers")
}

// partitionConsumer is a worker routine which consumes a single partition in the __consumer_offsets topic.
// It processes all it's messages and pushes the information into the storage module. Additionally it
// reports to the storage module when it has initially caught up the partition lag.
func (module *OffsetConsumer) partitionConsumer(consumer sarama.Consumer, partitionID int32) {
	defer module.wg.Done()

	log.Debugf("Starting consumer %d", partitionID)
	pconsumer, err := consumer.ConsumePartition(module.offsetsTopicName, partitionID, sarama.OffsetOldest)
	if err != nil {
		log.WithFields(log.Fields{
			"topic":     module.offsetsTopicName,
			"partition": partitionID,
			"error":     err.Error(),
		}).Panic("could not start consumer")
	}
	log.Debugf("Started consumer %d", partitionID)
	defer pconsumer.AsyncClose()

	ticker := time.NewTicker(5 * time.Second)
	var consumedOffset int64

	for {
		select {
		case msg := <-pconsumer.Messages():
			messagesInSuccess.WithLabelValues(msg.Topic).Add(1)
			module.processMessage(msg)
			consumedOffset = msg.Offset
		case err := <-pconsumer.Errors():
			messagesInFailed.WithLabelValues(err.Topic).Add(1)
			log.WithFields(log.Fields{
				"error":     err.Error(),
				"topic":     err.Topic,
				"partition": err.Partition,
			}).Errorf("partition consume error")
		case <-ticker.C:
			// Regularly check if we have completely consumed the offsets topic
			// If that's the case report it to our storage module
			var highWaterMark int64
			highWaterMark = math.MaxInt64
			offsetWaterMarks.Lock.RLock()
			if val, exists := offsetWaterMarks.PartitionsByID[partitionID]; exists {
				// Not sure why -1 is needed here, but otherwise there are lots of partition consumers with a remaining lag of 1
				highWaterMark = val.HighWaterMark - 1
				if highWaterMark < 0 {
					highWaterMark = 0
				}
			}
			offsetWaterMarks.Lock.RUnlock()
			if consumedOffset >= highWaterMark {
				request := newMarkOffsetPartitionReadyRequest(partitionID)
				module.storageChannel <- request
				ticker.Stop()
			} else {
				log.WithFields(log.Fields{
					"partition":       partitionID,
					"high_water_mark": highWaterMark,
					"consumed_offset": consumedOffset,
					"remaining_lag":   highWaterMark - consumedOffset,
				}).Debug("partition consumer has not caught up the lag yet")
			}
		}
	}
}

// processMessage decodes the message and sends it to the storage module
func (module *OffsetConsumer) processMessage(msg *sarama.ConsumerMessage) {
	logger := module.logger.WithFields(log.Fields{
		"offset_topic":     msg.Topic,
		"offset_partition": msg.Partition,
		"offset_offset":    msg.Offset,
	})

	key := bytes.NewBuffer(msg.Key)
	value := bytes.NewBuffer(msg.Value)

	// Get the key version which tells us what kind of message (group metadata or offset info) we have received
	var keyVersion int16
	err := binary.Read(key, binary.BigEndian, &keyVersion)
	if err != nil {
		logger.WithFields(log.Fields{
			"reason": "no key version",
		}).Warn("failed to decode offset message")
		return
	}

	switch keyVersion {
	case 0, 1:
		module.processOffsetCommit(key, value, logger)
	case 2:
		// module.processGroupMetadata(key, value, logger)
	default:
		logger.WithFields(log.Fields{
			"reason":  "unknown key version",
			"version": keyVersion,
		}).Warn("Failed to decode offset message")
	}
}

// processOffsetCommit decodes all offset commit messages and sends them to the storage module
func (module *OffsetConsumer) processOffsetCommit(key *bytes.Buffer, value *bytes.Buffer, logger *log.Entry) {
	isTombstone := false
	if value.Len() == 0 {
		isTombstone = true
	}

	// A tombstone on the __consumer_offsets topic indicates that the consumer group either expired
	// due too configured group retention or that the consumed topic has been deleted
	if isTombstone {
		offsetCommitTombstone.Add(1)
		group, err := readString(key)
		if err != nil {
			logger.WithFields(log.Fields{
				"error": err.Error(),
			}).Errorf("failed to read tombstone's consumer group")
			return
		}
		topic, err := readString(key)
		if err != nil {
			logger.WithFields(log.Fields{
				"error": err.Error(),
				"group": group,
				"topic": topic,
			}).Errorf("failed to read tombstone's topic")
			return
		}
		var partitionID int32
		err = binary.Read(key, binary.BigEndian, &partitionID)
		if err != nil {
			logger.WithFields(log.Fields{
				"error":     err.Error(),
				"group":     group,
				"partition": partitionID,
			}).Errorf("failed to read tombstone's partition")
			return
		}

		logger.WithFields(log.Fields{
			"group":     group,
			"topic":     topic,
			"partition": partitionID,
		}).Debug("received a tombstone")
		module.storageChannel <- newDeleteConsumerGroupRequest(group, topic, partitionID)

		return
	}

	offset, err := newConsumerPartitionOffset(key, value, logger)
	if err != nil {
		// Error is already logged inside of the function
		return
	}
	logger.WithFields(log.Fields{
		"group":     offset.Group,
		"topic":     offset.Topic,
		"partition": offset.Partition,
	}).Debug("received consumer offset")

	if !module.isTopicAllowed(offset.Topic) {
		logger.WithFields(log.Fields{
			"topic": offset.Topic,
		}).Debug("topic is not allowed")
		return
	}
	module.storageChannel <- newAddConsumerOffsetRequest(offset)
}

func (module *OffsetConsumer) isTopicAllowed(topicName string) bool {
	if module.options.IgnoreSystemTopics {
		if strings.HasPrefix(topicName, "__") || strings.HasPrefix(topicName, "_confluent") {
			return false
		}
	}

	return true
}

// processGroupMetadata decodes all group metadata messages and sends them to the storage module
func (module *OffsetConsumer) processGroupMetadata(key *bytes.Buffer, value *bytes.Buffer, logger *log.Entry) {
	isTombstone := false
	if value.Len() == 0 {
		isTombstone = true
	}
	if isTombstone {
		return
	}

	// Group metadata contains client information (such as owner's IP address), how many partitions are assigned to a group member etc
	metadata, err := newConsumerGroupMetadata(key, value, logger)
	if err != nil {
		// Error is already logged inside of the function
		return
	}
	module.storageChannel <- newAddGroupMetadata(metadata)
}
