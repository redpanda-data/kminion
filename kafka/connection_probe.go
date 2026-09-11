package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// connectionProbeRequestTimeout bounds a single broker's probe request. It is
// independent of the configured probe interval so one unreachable broker
// cannot delay the probes to the others or stall the next tick.
const connectionProbeRequestTimeout = 10 * time.Second

// brokerConnectionProber is the seam that makes the connection health check
// loop unit-testable without a live Kafka cluster: newLiveBrokerProber wraps a
// real kgo.Client, and tests inject a fake implementation.
type brokerConnectionProber interface {
	// ListBrokerIDs returns the node ID of every broker currently in the cluster.
	ListBrokerIDs(ctx context.Context) ([]int32, error)
	// ProbeBroker issues a request against a connection to the given broker
	// and returns an error if a working connection could not be established.
	ProbeBroker(ctx context.Context, brokerID int32) error
	// Close releases the resources held by the prober (e.g. the underlying
	// kgo.Client and any connections it opened).
	Close()
}

// connectionProbeMetrics are the Prometheus series the connection health
// check reports. These are new, additive metric names: they do not replace
// or alter any metric reported by the minion or end-to-end services.
type connectionProbeMetrics struct {
	attemptsTotal        *prometheus.CounterVec
	failuresTotal        *prometheus.CounterVec
	lastSuccessTimestamp *prometheus.GaugeVec
}

func newConnectionProbeMetrics(registerer prometheus.Registerer) *connectionProbeMetrics {
	m := &connectionProbeMetrics{
		attemptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: "kafka",
			Name:      "connection_probe_attempts_total",
			Help:      "Number of times kminion tried to open a fresh connection to a broker to verify it still accepts new Kafka connections",
		}, []string{"broker_id"}),
		failuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: "kafka",
			Name:      "connection_probe_failures_total",
			Help:      "Number of times kminion failed to open a fresh connection to a broker",
		}, []string{"broker_id"}),
		lastSuccessTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: "kafka",
			Name:      "connection_probe_last_success_timestamp_seconds",
			Help:      "Unix timestamp of the last time kminion successfully opened a fresh connection to a broker",
		}, []string{"broker_id"}),
	}

	registerer.MustRegister(m.attemptsTotal, m.failuresTotal, m.lastSuccessTimestamp)

	return m
}

// recordProbeResult updates the connection probe metrics for a single broker.
func recordProbeResult(metrics *connectionProbeMetrics, brokerID int32, probeErr error, now time.Time) {
	label := strconv.FormatInt(int64(brokerID), 10)
	metrics.attemptsTotal.WithLabelValues(label).Inc()

	if probeErr != nil {
		metrics.failuresTotal.WithLabelValues(label).Inc()
		return
	}

	metrics.lastSuccessTimestamp.WithLabelValues(label).Set(float64(now.Unix()))
}

// runConnectionHealthCheck runs probeAllBrokersOnce on a ticker until ctx is
// canceled. newProber is called at the start of every tick so each tick
// probes with brand new connections rather than reusing ones from the
// previous tick.
func runConnectionHealthCheck(ctx context.Context, newProber func(ctx context.Context) (brokerConnectionProber, error), interval time.Duration, metrics *connectionProbeMetrics, logger *zap.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeAllBrokersOnce(ctx, newProber, metrics, logger)
		}
	}
}

// probeAllBrokersOnce creates one prober, lists the current brokers, probes
// every broker concurrently with its own bounded timeout, records the result
// of each, and closes the prober before returning.
func probeAllBrokersOnce(ctx context.Context, newProber func(ctx context.Context) (brokerConnectionProber, error), metrics *connectionProbeMetrics, logger *zap.Logger) {
	prober, err := newProber(ctx)
	if err != nil {
		logger.Warn("connection health check: failed to create prober", zap.Error(err))
		return
	}
	defer prober.Close()

	brokerIDs, err := prober.ListBrokerIDs(ctx)
	if err != nil {
		logger.Warn("connection health check: failed to list brokers", zap.Error(err))
		return
	}

	now := time.Now()

	var wg sync.WaitGroup
	for _, brokerID := range brokerIDs {
		wg.Add(1)
		go func(brokerID int32) {
			defer wg.Done()

			probeCtx, cancel := context.WithTimeout(ctx, connectionProbeRequestTimeout)
			defer cancel()

			probeErr := prober.ProbeBroker(probeCtx, brokerID)
			if probeErr != nil {
				logger.Warn("connection health check: failed to probe broker",
					zap.Int32("broker_id", brokerID),
					zap.Error(probeErr))
			}

			recordProbeResult(metrics, brokerID, probeErr, now)
		}(brokerID)
	}
	wg.Wait()
}

// liveBrokerProber is the real brokerConnectionProber implementation, backed
// by a freshly created kgo.Client.
type liveBrokerProber struct {
	client *kgo.Client
	adm    *kadm.Client
}

// newLiveBrokerProber creates a brand new kgo.Client using the same
// TLS/SASL/etc settings as kminion's other Kafka clients, and wraps it as a
// brokerConnectionProber. The caller must call Close() on the returned
// prober once done with it.
func newLiveBrokerProber(cfg Config, logger *zap.Logger) (brokerConnectionProber, error) {
	kgoOpts, err := NewKgoConfig(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client config: %w", err)
	}

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &liveBrokerProber{
		client: client,
		adm:    kadm.NewClient(client),
	}, nil
}

func (p *liveBrokerProber) ListBrokerIDs(ctx context.Context) ([]int32, error) {
	brokers, err := p.adm.ListBrokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list brokers: %w", err)
	}

	ids := make([]int32, 0, len(brokers))
	for _, broker := range brokers {
		ids = append(ids, broker.NodeID)
	}

	return ids, nil
}

func (p *liveBrokerProber) ProbeBroker(ctx context.Context, brokerID int32) error {
	req := kmsg.NewApiVersionsRequest()
	resp, err := p.client.Broker(int(brokerID)).Request(ctx, &req)
	if err != nil {
		return fmt.Errorf("failed to request api versions from broker %d: %w", brokerID, err)
	}

	versionsResp, ok := resp.(*kmsg.ApiVersionsResponse)
	if !ok {
		return fmt.Errorf("unexpected response type %T for ApiVersionsRequest", resp)
	}

	if err := kerr.ErrorForCode(versionsResp.ErrorCode); err != nil {
		return fmt.Errorf("broker %d returned an error for api versions request: %w", brokerID, err)
	}

	return nil
}

func (p *liveBrokerProber) Close() {
	p.client.Close()
}
