package prometheus

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func (e *Exporter) collectClusterInfo(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.ClusterInfo.Enabled {
		return true
	}

	version, err := e.minionSvc.GetClusterVersion(ctx)
	if err != nil {
		e.logger.Error("failed to get kafka cluster version", zap.Error(err))
		return false
	}

	metadata, err := e.minionSvc.GetMetadataCached(ctx)
	if err != nil {
		e.logger.Error("failed to get kafka metadata", zap.Error(err))
		return false
	}
	brokerCount := len(metadata.Brokers)
	clusterID := ""
	if metadata.ClusterID != nil {
		clusterID = *metadata.ClusterID
	}

	ch <- prometheus.MustNewConstMetric(
		e.clusterInfo,
		prometheus.GaugeValue,
		1,
		version,
		strconv.Itoa(brokerCount),
		strconv.Itoa(int(metadata.ControllerID)),
		clusterID,
	)
	return true
}
