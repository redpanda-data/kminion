package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
)

func (e *Exporter) collectExporterMetrics(_ context.Context, ch chan<- prometheus.Metric) bool {
	recordsConsumed := e.minionSvc.GetNumberOfOffsetRecordsConsumed()
	ch <- prometheus.MustNewConstMetric(
		e.offsetConsumerRecordsConsumed,
		prometheus.CounterValue,
		recordsConsumed,
	)
	return true
}
