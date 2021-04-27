package minion

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func getBucket(cfg Config) []float64 {
	// Since this is an exponential bucket we need to take Log base2 or binary as the upper bound
	// Divide by 10 for the argument because the base is counted as 20ms and we want to normalize it as base 2 instead of 20
	// +2 because it starts at 5ms or 0.005 sec, to account 5ms and 10ms before it goes to the base which in this case is 0.02 sec or 20ms
	// and another +1 to account for decimal points on int parsing
	latencyCount := math.Logb(float64(cfg.EndToEnd.Consumer.LatencySla.Milliseconds() / 10))
	count := int(latencyCount) + 3
	bucket := prometheus.ExponentialBuckets(0.005, 2, count)

	return bucket
}

func initEndtoendLatencyHistogram(cfg Config, metricNamespace string) *prometheus.Histogram {
	histogram := promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Subsystem: "kafka",
		Name:      "endtoend_latency_seconds",
		Help:      "Time it has taken to consume a Kafka message via a Consumer Group which KMinion has produced before",
		Buckets:   getBucket(cfg),
	})

	return &histogram
}

func initCommitLatencyHistogram(cfg Config, metricNamespace string) *prometheus.Histogram {
	histogram := promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Subsystem: "kafka",
		Name:      "commit_latency_seconds",
		Help:      "Time it has taken to commit the message via a Consumer Group on KMinion Management Topic",
		Buckets:   getBucket(cfg),
	})

	return &histogram
}

func (s *Service) GetLatencyHistogram() *prometheus.Histogram {
	return s.endtoendLatencyHistogram
}

func (s *Service) observeLatencyHistogram(time float64, partition int) error {
	h := *s.endtoendLatencyHistogram
	h.Observe(time)
	return nil
}

func (s *Service) observeCommitLatencyHistogram(time float64, partition int) error {
	h := *s.commitLatencyHistogram
	h.Observe(time)
	return nil
}
