package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectLogDirs(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.LogDirs.Enabled {
		return true
	}
	hasErrors := false

	sizeByBroker := make(map[kgo.BrokerMetadata]int64)
	sizeByTopicName := make(map[string]int64)

	logDirsSharded := e.minionSvc.DescribeLogDirs(ctx)
	for _, logDirRes := range logDirsSharded {
		childLogger := e.logger.With(zap.String("broker_address", logDirRes.Broker.Host),
			zap.String("broker_id", strconv.Itoa(int(logDirRes.Broker.NodeID))))

		if logDirRes.Err != nil {
			childLogger.Error("failed to describe a broker's log dirs", zap.Error(logDirRes.Err))
			hasErrors = true
			continue
		}

		for _, dir := range logDirRes.LogDirs.Dirs {
			err := kerr.ErrorForCode(dir.ErrorCode)
			if err != nil {
				childLogger.Error("failed to describe a broker's log dir",
					zap.String("log_dir", dir.Dir),
					zap.Error(err))
				hasErrors = true
				continue
			}
			for _, topic := range dir.Topics {
				topicSize := int64(0)
				for _, partition := range topic.Partitions {
					topicSize += partition.Size
				}
				sizeByTopicName[topic.Topic] += topicSize
				sizeByBroker[logDirRes.Broker] += topicSize
			}
		}
	}

	// Report the total log dir size per broker
	for broker, size := range sizeByBroker {
		rackID := ""
		if broker.Rack != nil {
			rackID = *broker.Rack
		}
		ch <- prometheus.MustNewConstMetric(
			e.brokerLogDirSize,
			prometheus.GaugeValue,
			float64(size),
			strconv.Itoa(int(broker.NodeID)),
			broker.Host,
			strconv.Itoa(int(broker.Port)),
			rackID,
		)
	}

	// If one of the log dir responses returned an error we can not reliably report the topic log dirs, as there might
	// be additional data on the brokers that failed to respond.
	if hasErrors {
		return true
	}

	// Report the total log dir size per topic
	for topicName, size := range sizeByTopicName {
		ch <- prometheus.MustNewConstMetric(
			e.topicLogDirSize,
			prometheus.GaugeValue,
			float64(size),
			topicName,
		)
	}

	return hasErrors
}
