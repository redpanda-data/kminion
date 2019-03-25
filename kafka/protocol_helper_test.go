package kafka

import (
	"bytes"
	"testing"
)

func TestReadString(t *testing.T) {
	tables := []struct {
		buf  *bytes.Buffer
		want string
	}{
		{
			bytes.NewBufferString("\x00\x16console-consumer-36268"),
			"console-consumer-36268",
		},
		{
			bytes.NewBufferString("\x00\x04test"),
			"test",
		},
		{
			bytes.NewBufferString("\x00\x04test\x00\x05test2"),
			"test",
		},
	}

	for _, table := range tables {
		result, err := readString(table.buf)
		if err != nil {
			t.Errorf("Error while reading string %v: %v", table.buf.String(), err.Error())
		}
		if result != table.want {
			t.Errorf("Expected: %v , Got: %v", table.want, result)
		}
	}
}
