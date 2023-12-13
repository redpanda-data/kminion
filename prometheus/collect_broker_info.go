package prometheus

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func (e *Exporter) collectBrokerInfo(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.BrokerInfo.Enabled {
		return true
	}

	metadata, err := e.minionSvc.GetMetadataCached(ctx)
	if err != nil {
		e.logger.Error("failed to get kafka metadata", zap.Error(err))
		return false
	}

	for _, broker := range metadata.Brokers {
		rack := ""
		if broker.Rack != nil {
			rack = *broker.Rack
		}

		isController := metadata.ControllerID == broker.NodeID
		ch <- prometheus.MustNewConstMetric(
			e.brokerInfo,
			prometheus.GaugeValue,
			1,
			strconv.Itoa(int(broker.NodeID)),
			broker.Host,
			strconv.Itoa(int(broker.Port)),
			rack,
			strconv.FormatBool(isController),
		)
	}

	return true
}
