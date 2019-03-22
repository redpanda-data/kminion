package kafka

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
)

// OffsetConsumer is a consumer module which reads consumer group information from the offsets topic in a Kafka cluster.
// The offsets topic is typically named __consumer_offsets. All messages in this topic are binary and therefore they
// must first be decoded to access the information. This module consumes and processes all messages in the offsets topic.
type OffsetConsumer struct {
	// Waitgroup for all partitionConsumers. For each partition consumer waitgroup is incremented
	wg sync.WaitGroup

	// StorageChannel is used to persist processed messages in memory so that they can be exposed with prometheus
	storageChannel chan *StorageRequest

	logger           *log.Entry
	client           sarama.Client
	offsetsTopicName string
}

// NewOffsetConsumer creates a consumer which process all messages in the __consumer_offsets topic
// If it cannot connect to the cluster it will panic
func NewOffsetConsumer(opts *options.Options, storageChannel chan *StorageRequest) *OffsetConsumer {
	logger := log.WithFields(log.Fields{
		"module": "offset_consumer",
	})

	// Connect client to at least one of the brokers and verify the connection by requesting metadata
	connectionLogger := log.WithFields(log.Fields{
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
	}).Info("Starting consumers")
	for _, partition := range partitions {
		module.wg.Add(1)
		go module.partitionConsumer(consumer, partition)
	}
	log.WithFields(log.Fields{
		"topic": module.offsetsTopicName,
		"count": len(partitions),
	}).Info("Started all consumers")
}

// partitionConsumer is a worker routine which consumes a single partition in the __consumer_offsets topic
func (module *OffsetConsumer) partitionConsumer(consumer sarama.Consumer, partitionID int32) {
	defer module.wg.Done()

	log.Infof("Starting consumer %d", partitionID)
	pconsumer, err := consumer.ConsumePartition(module.offsetsTopicName, partitionID, sarama.OffsetOldest)
	if err != nil {
		log.WithFields(log.Fields{
			"topic":     module.offsetsTopicName,
			"partition": partitionID,
			"error":     err.Error(),
		}).Panic("could not start consumer")
	}
	log.Infof("Started consumer %d", partitionID)
	defer pconsumer.AsyncClose()

	counter := 0
	for {
		select {
		case msg := <-pconsumer.Messages():

			counter++
			if counter%10000 == 0 {
				log.WithFields(log.Fields{
					"partition_id": partitionID,
				}).Infof("Consumed '%d'", counter)
			}
			module.processConsumerOffsetsMessage(msg)
		case err := <-pconsumer.Errors():
			log.Errorf("consume error. %+v %+v %+v", err.Topic, err.Partition, err.Err.Error())
		}
	}
}

// processConsumerOffsetsMessage is responsible for decoding the consumer offsets message
func (module *OffsetConsumer) processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) {
	logger := log.WithFields(log.Fields{"offset_topic": msg.Topic, "offset_partition": msg.Partition, "offset_offset": msg.Offset})

	if len(msg.Value) == 0 {
		// Tombstone message - we don't handle them for now
		logger.Debug("dropped tombstone")
		return
	}

	// Get the key version which tells us what kind of message (group metadata or offset info) we have received
	var keyver int16
	keyBuffer := bytes.NewBuffer(msg.Key)
	err := binary.Read(keyBuffer, binary.BigEndian, &keyver)
	if err != nil {
		logger.Warn("Failed to decode offset message", log.Fields{"reason": "no key version"})
		return
	}
	switch keyver {
	case 0, 1:
		offset, err := processKeyAndOffset(keyBuffer, msg.Value, logger)
		if err != nil {
			break
		}
		if !isTopicAllowed(offset.Topic) {
			logger.Debug("topic is not allowed")
			return
		}
		module.storageChannel <- newAddConsumerOffsetRequest(offset)
	case 2:
		// processGroupMetadata(keyBuffer, msg.Value, logger)
	default:
		logger.Warn("Failed to decode offset message", log.Fields{"reason": "unknown key version", "version": keyver})
	}
}

func isTopicAllowed(topicName string) bool {
	if strings.HasPrefix(topicName, "__") || strings.HasPrefix(topicName, "_confluent") {
		return false
	}

	return true
}

func processKeyAndOffset(buffer *bytes.Buffer, value []byte, logger *log.Entry) (*ConsumerPartitionOffset, error) {
	offset, err := newConsumerPartitionOffset(buffer, value, logger)
	if err != nil {
		return nil, err
	}
	logger.Debugf("Group %v - Topic: %v - Partition: %v - Offset: %v", offset.Group, offset.Topic, offset.Partition, offset.Offset)

	return offset, nil
}

func processGroupMetadata(keyBuffer *bytes.Buffer, value []byte, logger *log.Entry) {
	// Group metadata contains client information (such as owner's IP address), how many partitions are assigned to a group member etc
	newConsumerGroupMetadata(keyBuffer, value, logger)
}
