package e2e

import (
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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
		c.logger.Debug("kafka connection failed", zap.String("broker_host", meta.Host), zap.Error(err))
		return
	}
	c.logger.Debug("kafka connection succeeded",
		zap.String("host", meta.Host),
		zap.Duration("dial_duration", dialDur))
}

func (c clientHooks) OnDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	c.logger.Debug("kafka broker disconnected",
		zap.String("host", meta.Host))
}

// OnRead is passed the broker metadata, the key for the response that
// was read, the number of bytes read, how long the Client waited
// before reading the response, how long it took to read the response,
// and any error.
//
// The bytes written does not count any tls overhead.
// OnRead is called after a read from a broker.
func (c clientHooks) OnRead(_ kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, _ error) {

}

// OnWrite is passed the broker metadata, the key for the request that
// was written, the number of bytes written, how long the request
// waited before being written, how long it took to write the request,
// and any error.
//
// The bytes written does not count any tls overhead.
// OnWrite is called after a write to a broker.
func (c clientHooks) OnWrite(_ kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, _ error) {

}
