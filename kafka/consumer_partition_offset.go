package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
)

// ConsumerPartitionOffset represents a consumer group commit which can be decoded from the consumer_offsets topic
type ConsumerPartitionOffset struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
}

type offsetValue struct {
	Offset    int64
	Timestamp int64
}

// newConsumerPartitionOffset decodes a key and value buffer to ConsumerPartitionOffset entry
func newConsumerPartitionOffset(key *bytes.Buffer, value *bytes.Buffer, logger *log.Entry) (*ConsumerPartitionOffset, error) {
	var err error
	entry := ConsumerPartitionOffset{}

	// Decode key which contains group, topic and partition information first
	entry.Group, err = readString(key)
	if err != nil {
		logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("failed to decode group from consumer offset")
		return nil, fmt.Errorf("could not decode group from offset key buffer: %v", err)
	}
	entry.Topic, err = readString(key)
	if err != nil {
		logger.WithFields(log.Fields{
			"group": entry.Group,
			"error": err.Error(),
		}).Error("failed to decode group from consumer offset")
		return nil, fmt.Errorf("could not decode topic from offset key buffer: %v", err)
	}
	err = binary.Read(key, binary.BigEndian, &entry.Partition)
	if err != nil {
		logger.WithFields(log.Fields{
			"group": entry.Group,
			"topic": entry.Topic,
			"error": err.Error(),
		}).Error("failed to decode partition from consumer offset")
		return nil, fmt.Errorf("could not decode partition from offset key buffer: %v", err)
	}

	offsetLogger := logger.WithFields(log.Fields{
		"message_type": "offset",
		"group":        entry.Group,
		"topic":        entry.Topic,
		"partition":    entry.Partition,
	})

	// Decode value
	// Decode value version so that we decode the message correctly
	var valueVersion int16
	err = binary.Read(value, binary.BigEndian, &valueVersion)
	if err != nil {
		offsetLogger.WithFields(log.Fields{
			"reason": "no value version",
		}).Warn("failed to decode")

		return nil, fmt.Errorf("message value has no version")
	}
	offsetCommit.WithLabelValues(strconv.Itoa(int(valueVersion))).Add(1)

	// Decode message value using the right decoding function for given version
	var decodedValue offsetValue
	switch valueVersion {
	case 0, 1:
		decodedValue, err = decodeOffsetValueV0(value, offsetLogger.WithField("value_version", valueVersion))
	case 3:
		decodedValue, err = decodeOffsetValueV3(value, offsetLogger.WithField("value_version", valueVersion))
	default:
		err = fmt.Errorf("unknown value version to decode offsetValue. Given version: '%v'", valueVersion)
	}
	if err != nil {
		return nil, err
	}
	entry.Offset = decodedValue.Offset
	entry.Timestamp = decodedValue.Timestamp

	return &entry, nil
}

func decodeOffsetValueV0(value *bytes.Buffer, logger *log.Entry) (offsetValue, error) {
	offset := offsetValue{}

	err := binary.Read(value, binary.BigEndian, &offset.Offset)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "offset",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offset, fmt.Errorf("failed to decode 'offset' field for OffsetValue V0: %v", err)
	}
	_, err = readString(value)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "metadata",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offset, fmt.Errorf("failed to decode 'metadata' field for OffsetValue V0: %v", err)
	}
	err = binary.Read(value, binary.BigEndian, &offset.Timestamp)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "timestamp",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offset, fmt.Errorf("failed to decode 'timestamp' field for OffsetValue V0: %v", err)
	}

	return offset, nil
}

func decodeOffsetValueV3(value *bytes.Buffer, logger *log.Entry) (offsetValue, error) {
	offsetValue := offsetValue{}

	err := binary.Read(value, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "offset",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offsetValue, fmt.Errorf("failed to decode 'offset' field for OffsetValue: %v", err)
	}

	// leaderEpoch refers to the number of leaders previously assigned by the controller.
	// Every time a leader fails, the controller selects the new leader, increments the current "leader epoch" by 1
	var leaderEpoch int32
	err = binary.Read(value, binary.BigEndian, &leaderEpoch)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "leaderEpoch",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offsetValue, fmt.Errorf("failed to decode 'leaderEpoch' field for OffsetValue V3: %v", err)
	}

	// metadata field contains additional metadata information which can optionally be set by a consumer
	_, err = readString(value)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "metadata",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offsetValue, fmt.Errorf("failed to decode 'metadata' field for OffsetValue V3: %v", err)
	}
	err = binary.Read(value, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		logger.WithFields(log.Fields{
			"error_at": "timestamp",
			"error":    err.Error(),
		}).Error("failed to decode offset value")
		return offsetValue, fmt.Errorf("failed to decode 'timestamp' field for OffsetValue: %v", err)
	}

	return offsetValue, nil
}
