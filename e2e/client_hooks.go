package e2e

import (
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// in e2e we only use client hooks for logging connect/disconnect messages
type clientHooks struct {
	logger *zap.Logger
}

func newEndToEndClientHooks(logger *zap.Logger) *clientHooks {

	logger = logger.With(zap.String("source", "end_to_end"))

	return &clientHooks{
		logger: logger,
	}
}

func (c clientHooks) OnConnect(meta kgo.BrokerMetadata, dialDur time.Duration, _ net.Conn, err error) {
	if err != nil {
		c.logger.Error("kafka connection failed", zap.String("broker_host", meta.Host), zap.Int32("broker_id", meta.NodeID), zap.Error(err))
		return
	}
	c.logger.Debug("kafka connection succeeded",
		zap.String("host", meta.Host), zap.Int32("broker_id", meta.NodeID),
		zap.Duration("dial_duration", dialDur))
}

func (c clientHooks) OnDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	c.logger.Warn("kafka broker disconnected", zap.Int32("broker_id", meta.NodeID),
		zap.String("host", meta.Host))
}

// OnWrite is passed the broker metadata, the key for the request that
// was written, the number of bytes written, how long the request
// waited before being written, how long it took to write the request,
// and any error.
//
// The bytes written does not count any tls overhead.
// OnWrite is called after a write to a broker.
//
// OnWrite(meta BrokerMetadata, key int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error)
func (c clientHooks) OnWrite(meta kgo.BrokerMetadata, key int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	keyName := kmsg.NameForKey(key)
	if keyName != "OffsetCommit" {
		return
	}

	c.logger.Info("hooks onWrite",
		zap.Duration("timeToWrite", timeToWrite),
		zap.NamedError("err", err))

	offsetCommitStarted = time.Now()
}

var (
	offsetCommitStarted time.Time
)

// OnRead is passed the broker metadata, the key for the response that
// was read, the number of bytes read, how long the Client waited
// before reading the response, how long it took to read the response,
// and any error.
//
// The bytes written does not count any tls overhead.
// OnRead is called after a read from a broker.
// OnRead(meta BrokerMetadata, key int16, bytesRead int, readWait, timeToRead time.Duration, err error)
func (c clientHooks) OnRead(meta kgo.BrokerMetadata, key int16, bytesRead int, readWait, timeToRead time.Duration, err error) {

	keyName := kmsg.NameForKey(key)
	if keyName != "OffsetCommit" {
		return
	}

	dur := time.Since(offsetCommitStarted)

	c.logger.Info("hooks onRead",
		zap.Int64("timeToReadMs", timeToRead.Milliseconds()),
		zap.Int64("totalTime", dur.Milliseconds()),
		zap.NamedError("err", err))

}
