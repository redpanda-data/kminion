package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectConsumerGroups(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.ConsumerGroups.Enabled {
		return true
	}
	groups, err := e.minionSvc.DescribeConsumerGroups(ctx)
	if err != nil {
		e.logger.Error("failed to collect consumer groups, because Kafka request failed", zap.Error(err))
		return false
	}

	for _, group := range groups.Groups {
		err := kerr.ErrorForCode(group.ErrorCode)
		if err != nil {
			e.logger.Warn("consumer group could not be described", zap.Error(err))
			continue
		}

		val := 0
		if group.State == "Stable" {
			val = 1
		}

		ch <- prometheus.MustNewConstMetric(
			e.consumerGroupInfo,
			prometheus.GaugeValue,
			float64(val),
			group.Group,
			strconv.Itoa(len(group.Members)),
			group.Protocol,
			group.ProtocolType,
			group.State,
		)
	}
	return true
}
