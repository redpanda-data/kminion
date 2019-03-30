package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Protocol primitives helper file, see:
// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProtocolPrimitiveTypes

// readString tries to read a string following the Kafka binary protocol. Strings are size delimited.
// It returns an error if it can not read a string on the given buffer.
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
		return "", fmt.Errorf("string underflow")
	}
	return string(strbytes), nil
}
