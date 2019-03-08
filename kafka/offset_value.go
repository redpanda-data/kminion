package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type offsetValue struct {
	Offset    int64
	Timestamp int64
	ErrorAt   string
}

func newOffsetValue(offsetKey *offsetKey, valueBuffer *bytes.Buffer, version int16) (*offsetValue, error) {
	offset := offsetValue{}
	var err error

	switch version {
	case 0, 1:
		offset, err = decodeOffsetValueV0(valueBuffer)
	case 3:
		offset, err = decodeOffsetValueV3(valueBuffer)
	default:
		err = fmt.Errorf("unknown value version to decode offsetValue. Given version: '%v'", version)
	}

	if err != nil {
		return nil, err
	}

	return &offset, nil
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
