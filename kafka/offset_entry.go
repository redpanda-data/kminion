package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	log "github.com/sirupsen/logrus"
)

// OffsetEntry represents a consumer group commit which can be decoded from the consumer_offsets topic
type OffsetEntry struct {
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

func newOffsetEntry(buffer *bytes.Buffer, value []byte, logger *log.Entry) (*OffsetEntry, error) {
	// Decode key which contains group, topic and partition information first
	var err error
	entry := OffsetEntry{}

	entry.Group, err = readString(buffer)
	if err != nil {
		return nil, fmt.Errorf("could not decode group from offset key buffer: %v", err)
	}
	entry.Topic, err = readString(buffer)
	if err != nil {
		return nil, fmt.Errorf("could not decode topic from offset key buffer: %v", err)
	}
	err = binary.Read(buffer, binary.BigEndian, &entry.Partition)
	if err != nil {
		return nil, fmt.Errorf("could not decode partition from offset key buffer: %v", err)
	}

	offsetLogger := logger.WithFields(log.Fields{
		"message_type": "offset",
		"group":        entry.Group,
		"topic":        entry.Topic,
		"partition":    entry.Partition,
	})

	// Decode value version so that we decode the message correctly
	var valueVersion int16
	valueBuffer := bytes.NewBuffer(value)
	err = binary.Read(valueBuffer, binary.BigEndian, &valueVersion)
	if err != nil {
		offsetLogger.WithFields(log.Fields{
			"reason": "no value version",
		}).Warn("failed to decode")

		return nil, fmt.Errorf("message value has no version")
	}

	// Decode message value using the right decoding function for given version
	var decodedValue offsetValue
	switch valueVersion {
	case 0, 1:
		decodedValue, err = decodeOffsetValueV0(valueBuffer)
	case 3:
		decodedValue, err = decodeOffsetValueV3(valueBuffer)
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

func decodeOffsetValueV0(valueBuffer *bytes.Buffer) (offsetValue, error) {
	offset := offsetValue{}

	err := binary.Read(valueBuffer, binary.BigEndian, &offset.Offset)
	if err != nil {
		return offset, fmt.Errorf("failed to decode 'offset' field for OffsetValue V0: %v", err)
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offset, fmt.Errorf("failed to decode 'metadata' field for OffsetValue V0: %v", err)
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offset.Timestamp)
	if err != nil {
		return offset, fmt.Errorf("failed to decode 'timestamp' field for OffsetValue V0: %v", err)
	}

	return offset, nil
}

func decodeOffsetValueV3(valueBuffer *bytes.Buffer) (offsetValue, error) {
	offsetValue := offsetValue{}

	err := binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Offset)
	if err != nil {
		return offsetValue, fmt.Errorf("failed to decode 'offset' field for OffsetValue: %v", err)
	}
	var leaderEpoch int32
	err = binary.Read(valueBuffer, binary.BigEndian, &leaderEpoch)
	if err != nil {
		return offsetValue, fmt.Errorf("failed to decode 'leaderEpoch' field for OffsetValue V3: %v", err)
	}
	_, err = readString(valueBuffer)
	if err != nil {
		return offsetValue, fmt.Errorf("failed to decode 'metadata' field for OffsetValue V3: %v", err)
	}
	err = binary.Read(valueBuffer, binary.BigEndian, &offsetValue.Timestamp)
	if err != nil {
		return offsetValue, fmt.Errorf("failed to decode 'timestamp' field for OffsetValue: %v", err)
	}

	return offsetValue, nil
}
