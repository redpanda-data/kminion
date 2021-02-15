package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectBrokerInfo(ctx context.Context, ch chan<- prometheus.Metric) bool {
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
