package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
)

func (e *Exporter) collectEndToEnd(ctx context.Context, ch chan<- prometheus.Metric) bool {
	produceMs, exists := e.minionSvc.ProduceDurationMs(ctx)
	if exists && produceMs <= e.minionSvc.Cfg.EndToEnd.Producer.LatencySla.Milliseconds() {
		ch <- prometheus.MustNewConstMetric(e.endToEndProducerUp, prometheus.GaugeValue, 1.0)
	} else {
		ch <- prometheus.MustNewConstMetric(e.endToEndProducerUp, prometheus.GaugeValue, 0.0)
	}
	return true
}
