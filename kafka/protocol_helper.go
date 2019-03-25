package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

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
