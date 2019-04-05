package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
)

// ConsumerGroupMetadata contains additional information about consumer groups, such as:
// - Partition assignments (which hosts are assigned to partitions)
// - Session timeouts (hosts which haven't sent the keep alive in time)
// - Group rebalancing
type ConsumerGroupMetadata struct {
	Group   string
	Header  metadataHeader
	Members []metadataMember
}

type metadataHeader struct {
	ProtocolType string
	Generation   int32  // Upon every completion of the join group phase, the coordinator increments a GenerationId for the group
	Protocol     string // ProtocolName (e. g. "consumer")
	Leader       string
	Timestamp    int64
}

type metadataMember struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	RebalanceTimeout int32
	SessionTimeout   int32
	Assignment       map[string][]int32
}

// newConsumerGroupMetadata decodes a kafka message (key and value) to return an instance of
// the struct consumerGroupMetadata. It returns an error if it could not completely decode
// the message.
func newConsumerGroupMetadata(key *bytes.Buffer, value *bytes.Buffer, logger *log.Entry) (*ConsumerGroupMetadata, error) {
	// Decode key (resolves to group id)
	group, err := readString(key)
	if err != nil {
		logger.WithFields(log.Fields{
			"message_type": "metadata",
			"reason":       "group",
		}).Warn("failed to decode")
		return nil, err
	}

	// Decode value version
	var valueVersion int16
	err = binary.Read(value, binary.BigEndian, &valueVersion)
	if err != nil {
		logger.WithFields(log.Fields{
			"message_type": "metadata",
			"reason":       "no value version",
			"group":        group,
		}).Warn("failed to decode")

		return nil, err
	}
	groupMetadata.WithLabelValues(strconv.Itoa(int(valueVersion))).Add(1)

	// Decode value content
	var metadata *ConsumerGroupMetadata
	switch valueVersion {
	case 0, 1, 2:
		metadata, err = decodeGroupMetadata(valueVersion, group, value, logger.WithFields(log.Fields{
			"message_type": "metadata",
			"group":        group,
		}))
	default:
		logger.WithFields(log.Fields{
			"message_type": "metadata",
			"group":        group,
			"reason":       "value version",
			"version":      valueVersion,
		}).Warn("failed to decode")

		return nil, fmt.Errorf("Failed to decode group metadata because value version is not supported")
	}

	if err != nil {
		return nil, err
	}

	return metadata, err
}

func decodeGroupMetadata(valueVersion int16, group string, valueBuffer *bytes.Buffer, logger *log.Entry) (*ConsumerGroupMetadata, error) {
	// First decode header fields
	var err error
	metadataHeader := metadataHeader{}
	metadataHeader.ProtocolType, err = readString(valueBuffer)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "metadata header protocol type",
		}).Warn("failed to decode")
		return nil, err
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &metadataHeader.Generation)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at":      "metadata header generation",
			"error":         err.Error(),
			"protocol_type": metadataHeader.ProtocolType,
		}).Warn("failed to decode")
		return nil, err
	}
	metadataHeader.Protocol, err = readString(valueBuffer)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at":      "metadata header protocol",
			"protocol_type": metadataHeader.ProtocolType,
			"generation":    metadataHeader.Generation,
		}).Warn("failed to decode")
		return nil, err
	}
	metadataHeader.Leader, err = readString(valueBuffer)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at":      "metadata header leader",
			"protocol_type": metadataHeader.ProtocolType,
			"generation":    metadataHeader.Generation,
			"protocol":      metadataHeader.Protocol,
		}).Warn("failed to decode")
		return nil, err
	}

	if valueVersion >= 2 {
		err = binary.Read(valueBuffer, binary.BigEndian, &metadataHeader.Timestamp)
		if err != nil {
			logger.WithFields(log.Fields{
				"error_at":      "metadata header timestamp",
				"protocol_type": metadataHeader.ProtocolType,
				"generation":    metadataHeader.Generation,
				"protocol":      metadataHeader.Protocol,
				"timestamp":     metadataHeader.Timestamp,
			}).Warn("failed to decode")
			return nil, err
		}
	}

	// Now decode metadata members
	metadataLogger := logger.WithFields(log.Fields{
		"protocol_type": metadataHeader.ProtocolType,
		"generation":    metadataHeader.Generation,
		"protocol":      metadataHeader.Protocol,
		"leader":        metadataHeader.Leader,
		"timestamp":     metadataHeader.Timestamp,
	})

	var memberCount int32
	err = binary.Read(valueBuffer, binary.BigEndian, &memberCount)
	if err != nil {
		metadataLogger.WithFields(log.Fields{
			"error_at": "member count",
			"reason":   "no member size",
		}).Warn("failed to decode")
		return nil, err
	}

	members := make([]metadataMember, 0)
	for i := 0; i < int(memberCount); i++ {
		member, errorAt := decodeMetadataMember(valueBuffer, valueVersion)
		if errorAt != "" {
			metadataLogger.WithFields(log.Fields{
				"error_at": "metadata member",
				"reason":   errorAt,
			}).Warn("failed to decode")

			return nil, fmt.Errorf("Decoding member, error at: %v", errorAt)
		}
		members = append(members, member)
	}

	return &ConsumerGroupMetadata{
		Group:   group,
		Header:  metadataHeader,
		Members: members,
	}, nil
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
	if memberVersion >= 1 {
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
