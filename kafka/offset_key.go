package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type offsetKey struct {
	Group     string
	Topic     string
	Partition int32
	ErrorAt   string
}

func newOffsetKey(buf *bytes.Buffer) (*offsetKey, error) {
	var err error
	offsetKey := offsetKey{}

	offsetKey.Group, err = readString(buf)
	if err != nil {
		return &offsetKey, fmt.Errorf("could not decode group from offset key buffer: %v", err)
	}
	offsetKey.Topic, err = readString(buf)
	if err != nil {
		return &offsetKey, fmt.Errorf("could not decode topic from offset key buffer: %v", err)
	}
	err = binary.Read(buf, binary.BigEndian, &offsetKey.Partition)
	if err != nil {
		return &offsetKey, fmt.Errorf("could not decode partition from offset key buffer: %v", err)
	}
	return &offsetKey, nil
}
