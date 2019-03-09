package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"sync"
)

// OffsetConsumer is a consumer module which reads consumer group information from the offsets topic in a Kafka cluster.
// The offsets topic is typically named __consumer_offsets. All messages in this topic are binary and therefore they
// must first be decoded to access the information. This module consumes and processes all messages in the offsets topic.
type OffsetConsumer struct {
	// Waitgroup for all partitionConsumers. For each partition consumer waitgroup is incremented
	wg sync.WaitGroup
	// QuitChannel is being sent to when a partitionConsumer can not consume messages anymore
	quitChannel      chan struct{}
	client           sarama.Client
	offsetsTopicName string
}

// NewOffsetConsumer creates a consumer which process all messages in the __consumer_offsets topic
func NewOffsetConsumer(opts *options.Options) (*OffsetConsumer, error) {
	log.Debug("Trying to create a sarama client config")
	clientConfig := saramaClientConfig(opts)
	log.Debug("Trying to create a new sarama client")

	// Connect client to at least one of the brokers and verify the connection by requesting metadata
	client, err := sarama.NewClient(opts.KafkaBrokers, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start client: %v", err)
	}

	return &OffsetConsumer{
		wg:               sync.WaitGroup{},
		quitChannel:      make(chan struct{}),
		client:           client,
		offsetsTopicName: opts.ConsumerOffsetsTopicName,
	}, nil
}

// Start creates partition consumer for each partition in that topic and starts consuming them
func (module *OffsetConsumer) Start() error {
	// Create the consumer from the client
	consumer, err := sarama.NewConsumerFromClient(module.client)
	if err != nil {
		log.Error("failed to get new consumer", err)
		module.client.Close()
		return err
	}

	// Get the partition count for the offsets topic
	partitions, err := module.client.Partitions(module.offsetsTopicName)
	if err != nil {
		log.WithFields(log.Fields{
			"topic": module.offsetsTopicName,
			"error": err.Error(),
		}).Error("failed to get partition count")
		module.client.Close()
		return err
	}

	// Default to bootstrapping the offsets topic, unless configured otherwise
	startFrom := sarama.OffsetOldest

	// Start consumers for each partition with fan in
	log.WithFields(log.Fields{
		"topic": module.offsetsTopicName,
		"count": len(partitions),
	}).Info("Starting consumers")
	for i, partition := range partitions {
		pconsumer, err := consumer.ConsumePartition(module.offsetsTopicName, partition, startFrom)
		if err != nil {
			log.WithFields(log.Fields{
				"topic":     module.offsetsTopicName,
				"partition": i,
				"error":     err.Error(),
			})
			return err
		}
		module.wg.Add(1)
		go module.partitionConsumer(pconsumer)
	}

	return nil
}

// partitionConsumer is a worker routine which consumes a single partition in the __consumer_offsets topic
func (module *OffsetConsumer) partitionConsumer(consumer sarama.PartitionConsumer) {
	defer module.wg.Done()
	defer consumer.AsyncClose()

	for {
		select {
		case msg := <-consumer.Messages():
			module.processConsumerOffsetsMessage(msg)
		case err := <-consumer.Errors():
			log.Errorf("consume error. %+v %+v %+v", err.Topic, err.Partition, err.Err.Error())
		case <-module.quitChannel:
			return
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
		processKeyAndOffset(keyBuffer, msg.Value, logger)
	case 2:
		processGroupMetadata(keyBuffer, msg.Value, logger)
	default:
		logger.Warn("Failed to decode offset message", log.Fields{"reason": "unknown key version", "version": keyver})
	}
}

func processKeyAndOffset(buffer *bytes.Buffer, value []byte, logger *log.Entry) {
	offset, err := newOffsetEntry(buffer, value, logger)
	if err != nil {
		return
	}

	logger.Infof("Group %v - Topic: %v - Partition: %v - Offset: %v", offset.Group, offset.Topic, offset.Partition, offset.Offset)
}

func processGroupMetadata(keyBuffer *bytes.Buffer, value []byte, logger *log.Entry) {
	// Group metadata contains client information (such as owner's IP address), how many partitions are assigned to a group member etc
	newOffsetGroupMetadata(keyBuffer, value, logger)
}
