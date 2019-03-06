package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"sync"
)

// OffsetConsumer consumes the offset topic
type OffsetConsumer struct {
	wg               sync.WaitGroup
	client           sarama.Client
	quitChannel      chan struct{}
	offsetsTopicName string
}

type offsetKey struct {
	Group     string
	Topic     string
	Partition int32
	ErrorAt   string
}
type offsetValue struct {
	Offset    int64
	Timestamp int64
	ErrorAt   string
}
type metadataHeader struct {
	ProtocolType string
	Generation   int32
	Protocol     string
	Leader       string
}
type metadataMember struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	RebalanceTimeout int32
	SessionTimeout   int32
	Assignment       map[string][]int32
}

// NewOffsetConsumer creates a new kafka consumer
func NewOffsetConsumer(opts *options.Options) (*OffsetConsumer, error) {
	// Connect Kafka client
	log.Debug("Trying to create a sarama client config")
	clientConfig := getSaramaClientConfig(opts)
	log.Debug("Trying to create a new sarama client")
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

// StartKafkaConsumer starts consuming all partitions of the consumer offset topic
func (module *OffsetConsumer) StartKafkaConsumer() error {
	// Create the consumer from the client
	consumer, err := sarama.NewConsumerFromClient(module.client)
	if err != nil {
		log.Error("failed to get new consumer", err)
		module.client.Close()
		return err
	}

	// Get a partition count for the consumption topic
	partitions, err := module.client.Partitions(module.offsetsTopicName)
	if err != nil {
		log.WithFields(log.Fields{
			"topic": "__consumer_offsets",
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

	var keyver int16
	keyBuffer := bytes.NewBuffer(msg.Key)
	err := binary.Read(keyBuffer, binary.BigEndian, &keyver)
	if err != nil {
		logger.Warn("Failed to decode offset message", log.Fields{"reason": "no key version"})
		return
	}

	switch keyver {
	case 0, 1:
		module.decodeKeyAndOffset(keyBuffer, msg.Value, logger)
	case 2:
		module.decodeGroupMetadata(keyBuffer, msg.Value, logger)
	default:
		logger.Warn("Failed to decode offset message", log.Fields{"reason": "unknown key version", "version": keyver})
	}
}

func (module *OffsetConsumer) acceptConsumerGroup(group string) bool {
	return true
}

func (module *OffsetConsumer) decodeKeyAndOffset(keyBuffer *bytes.Buffer, value []byte, logger *log.Entry) {
	// Version 0 and 1 keys are decoded the same way
	offsetKey, errorAt := decodeOffsetKeyV0(keyBuffer)
	if errorAt != "" {
		logger.WithFields(log.Fields{"message_type": "offset",
			"group":     offsetKey.Group,
			"topic":     offsetKey.Topic,
			"partition": offsetKey.Partition,
			"reason":    errorAt,
		}).Warn("failed to decode")
		return
	}

	offsetLogger := logger.WithFields(log.Fields{
		"message_type": "offset",
		"group":        offsetKey.Group,
		"topic":        offsetKey.Topic,
		"partition":    offsetKey.Partition,
	})

	if !module.acceptConsumerGroup(offsetKey.Group) {
		offsetLogger.WithFields(log.Fields{
			"reason": "whitelist",
		}).Debug("dropped consumer group metrics")
		return
	}

	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err := binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		offsetLogger.WithFields(log.Fields{
			"reason": "no value version",
		}).Warn("failed to decode")
		return
	}

	switch valueVersion {
	case 0, 1:
		module.decodeAndSendOffset(offsetKey, valueBuffer, offsetLogger, decodeOffsetValueV0)
	case 3:
		module.decodeAndSendOffset(offsetKey, valueBuffer, offsetLogger, decodeOffsetValueV3)
	default:
		offsetLogger.WithFields(log.Fields{
			"reason":  "value version",
			"version": valueVersion,
		}).Warn("failed to decode")
	}
}

func (module *OffsetConsumer) decodeAndSendOffset(offsetKey offsetKey, valueBuffer *bytes.Buffer, logger *log.Entry, decoder func(*bytes.Buffer) (offsetValue, string)) {
	offsetValue, errorAt := decoder(valueBuffer)
	if errorAt != "" {
		logger.WithFields(log.Fields{
			"offset":    offsetValue.Offset,
			"timestamp": offsetValue.Timestamp,
			"reason":    errorAt,
		}).Warn("failed to decode")
		return
	}

	/*
		partitionOffset := &protocol.StorageRequest{
			RequestType: protocol.StorageSetConsumerOffset,
			Cluster:     module.cluster,
			Topic:       offsetKey.Topic,
			Partition:   int32(offsetKey.Partition),
			Group:       offsetKey.Group,
			Timestamp:   int64(offsetValue.Timestamp),
			Offset:      int64(offsetValue.Offset),
		}*/
	logger.WithFields(log.Fields{
		"offset":    offsetValue.Offset,
		"timestamp": offsetValue.Timestamp,
	}).Debug("consumer offset")
	// helpers.TimeoutSendStorageRequest(module.App.StorageChannel, partitionOffset, 1)
}

func (module *OffsetConsumer) decodeGroupMetadata(keyBuffer *bytes.Buffer, value []byte, logger *log.Entry) {
	group, err := readString(keyBuffer)
	if err != nil {
		logger.WithFields(log.Fields{
			"message_type": "metadata",
			"reason":       "group",
		}).Warn("failed to decode")
		return
	}

	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err = binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		logger.WithFields(log.Fields{
			"message_type": "metadata",
			"group":        group,
			"reason":       "no value version",
		}).Warn("failed to decode")
		return
	}

	switch valueVersion {
	case 0, 1:
		module.decodeAndSendGroupMetadata(valueVersion, group, valueBuffer, logger.WithFields(log.Fields{
			"message_type": "metadata",
			"group":        group,
		}))
	case 2:
		log.Infof("Discarding group metadata message with key version 2")
	default:
		logger.WithFields(log.Fields{
			"message_type": "metadata",
			"group":        group,
			"reason":       "value version",
			"version":      valueVersion,
		}).Warn("failed to decode")
	}
}

func (module *OffsetConsumer) decodeAndSendGroupMetadata(valueVersion int16, group string, valueBuffer *bytes.Buffer, logger *log.Entry) {
	metadataHeader, errorAt := decodeMetadataValueHeader(valueBuffer)
	metadataLogger := logger.WithFields(log.Fields{
		"protocol_type": metadataHeader.Protocol,
		"generation":    metadataHeader.Generation,
		"protocol":      metadataHeader.Protocol,
		"leader":        metadataHeader.Leader,
	})
	if errorAt != "" {
		metadataLogger.WithFields(log.Fields{
			"reason": errorAt,
		})
		return
	}

	var memberCount int32
	err := binary.Read(valueBuffer, binary.BigEndian, &memberCount)
	if err != nil {
		metadataLogger.WithFields(log.Fields{
			"reason": "no member size",
		}).Warn("failed to decode")
		return
	}

	// If memberCount is zero, clear all ownership
	if memberCount == 0 {
		metadataLogger.Debug("clear owners")
		/*
			helpers.TimeoutSendStorageRequest(module.App.StorageChannel, &protocol.StorageRequest{
				RequestType: protocol.StorageClearConsumerOwners,
				Cluster:     module.cluster,
				Group:       group,
			}, 1)
		*/
		return
	}

	count := int(memberCount)
	for i := 0; i < count; i++ {
		member, errorAt := decodeMetadataMember(valueBuffer, valueVersion)
		if errorAt != "" {
			metadataLogger.WithFields(log.Fields{
				"reason": errorAt,
			}).Warn("failed to decode")
			return
		}

		metadataLogger.Debug("group metadata")
		for topic, partitions := range member.Assignment {
			for _, partition := range partitions {
				log.WithFields(log.Fields{
					"Topic":     topic,
					"Partition": partition,
					"Group":     group,
					"Owner":     member.ClientHost,
				}).Info("Group metadata")
				/*
					helpers.TimeoutSendStorageRequest(module.App.StorageChannel, &protocol.StorageRequest{
						RequestType: protocol.StorageSetConsumerOwner,
						Cluster:     module.cluster,
						Topic:       topic,
						Partition:   partition,
						Group:       group,
						Owner:       member.ClientHost,
					}, 1)
				*/
			}
		}
	}
}

func decodeMetadataValueHeader(buf *bytes.Buffer) (metadataHeader, string) {
	var err error
	metadataHeader := metadataHeader{}

	metadataHeader.ProtocolType, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol_type"
	}
	err = binary.Read(buf, binary.BigEndian, &metadataHeader.Generation)
	if err != nil {
		return metadataHeader, "generation"
	}
	metadataHeader.Protocol, err = readString(buf)
	if err != nil {
		return metadataHeader, "protocol"
	}
	metadataHeader.Leader, err = readString(buf)
	if err != nil {
		return metadataHeader, "leader"
	}
	return metadataHeader, ""
}

func decodeMetadataMember(buf *bytes.Buffer, memberVersion int16) (metadataMember, string) {
	var err error
	memberMetadata := metadataMember{}

	memberMetadata.MemberID, err = readString(buf)
	if err != nil {
		return memberMetadata, "member_id"
	}
	memberMetadata.ClientID, err = readString(buf)
	if err != nil {
		return memberMetadata, "client_id"
	}
	memberMetadata.ClientHost, err = readString(buf)
	if err != nil {
		return memberMetadata, "client_host"
	}
	if memberVersion == 1 {
		err = binary.Read(buf, binary.BigEndian, &memberMetadata.RebalanceTimeout)
		if err != nil {
			return memberMetadata, "rebalance_timeout"
		}
	}
	err = binary.Read(buf, binary.BigEndian, &memberMetadata.SessionTimeout)
	if err != nil {
		return memberMetadata, "session_timeout"
	}

	var subscriptionBytes int32
	err = binary.Read(buf, binary.BigEndian, &subscriptionBytes)
	if err != nil {
		return memberMetadata, "subscription_bytes"
	}
	if subscriptionBytes > 0 {
		buf.Next(int(subscriptionBytes))
	}

	var assignmentBytes int32
	err = binary.Read(buf, binary.BigEndian, &assignmentBytes)
	if err != nil {
		return memberMetadata, "assignment_bytes"
	}

	if assignmentBytes > 0 {
		assignmentData := buf.Next(int(assignmentBytes))
		assignmentBuf := bytes.NewBuffer(assignmentData)
		var consumerProtocolVersion int16
		err = binary.Read(assignmentBuf, binary.BigEndian, &consumerProtocolVersion)
		if err != nil {
			return memberMetadata, "consumer_protocol_version"
		}
		if consumerProtocolVersion < 0 {
			return memberMetadata, "consumer_protocol_version"
		}
		assignment, errorAt := decodeMemberAssignmentV0(assignmentBuf)
		if errorAt != "" {
			return memberMetadata, "assignment"
		}
		memberMetadata.Assignment = assignment
	}

	return memberMetadata, ""
}

func decodeMemberAssignmentV0(buf *bytes.Buffer) (map[string][]int32, string) {
	var err error
	var topics map[string][]int32
	var numTopics, numPartitions, partitionID, userDataLen int32

	err = binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return topics, "assignment_topic_count"
	}

	topicCount := int(numTopics)
	topics = make(map[string][]int32, numTopics)
	for i := 0; i < topicCount; i++ {
		topicName, err := readString(buf)
		if err != nil {
			return topics, "topic_name"
		}

		err = binary.Read(buf, binary.BigEndian, &numPartitions)
		if err != nil {
			return topics, "assignment_partition_count"
		}
		partitionCount := int(numPartitions)
		topics[topicName] = make([]int32, numPartitions)
		for j := 0; j < partitionCount; j++ {
			err = binary.Read(buf, binary.BigEndian, &partitionID)
			if err != nil {
				return topics, "assignment_partition_id"
			}
			topics[topicName][j] = int32(partitionID)
		}
	}

	err = binary.Read(buf, binary.BigEndian, &userDataLen)
	if err != nil {
		return topics, "user_bytes"
	}
	if userDataLen > 0 {
		buf.Next(int(userDataLen))
	}

	return topics, ""
}

func decodeOffsetKeyV0(buf *bytes.Buffer) (offsetKey, string) {
	var err error
	offsetKey := offsetKey{}

	offsetKey.Group, err = readString(buf)
	if err != nil {
		return offsetKey, "group"
	}
	offsetKey.Topic, err = readString(buf)
	if err != nil {
		return offsetKey, "topic"
	}
	err = binary.Read(buf, binary.BigEndian, &offsetKey.Partition)
	if err != nil {
		return offsetKey, "partition"
	}
	return offsetKey, ""
}

func decodeOffsetValueV0(valueBuffer *bytes.Buffer) (offsetValue, string) {
	var err error
	offsetValue := offsetValue{}

	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		return offsetValue, "offset"
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offsetValue, "metadata"
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		return offsetValue, "timestamp"
	}
	return offsetValue, ""
}

func decodeOffsetValueV3(valueBuffer *bytes.Buffer) (offsetValue, string) {
	var err error
	offsetValue := offsetValue{}

	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		return offsetValue, "offset"
	}
	var leaderEpoch int32
	err = binary.Read(valueBuffer, binary.BigEndian, &leaderEpoch)
	if err != nil {
		return offsetValue, "leaderEpoch"
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offsetValue, "metadata"
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		return offsetValue, "timestamp"
	}
	return offsetValue, ""
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen int16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	if strlen == -1 {
		return "", nil
	}

	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}
