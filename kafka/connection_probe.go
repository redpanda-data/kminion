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
	tickFailuresTotal    prometheus.Counter
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
		tickFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Subsystem: "kafka",
			Name:      "connection_probe_tick_failures_total",
			Help:      "Number of connection health check ticks that failed before any broker could be probed (e.g. failed to create a client or list brokers)",
		}),
	}

	registerer.MustRegister(m.attemptsTotal, m.failuresTotal, m.lastSuccessTimestamp, m.tickFailuresTotal)

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

// runConnectionHealthCheck runs probeAllBrokersOnce once immediately and then
// again on every tick of a ticker, until ctx is canceled. Probing immediately
// on startup means a crash-looping process still gets at least one probe
// recorded, rather than waiting a full interval for the first result.
// newProber is called at the start of every tick so each tick probes with
// brand new connections rather than reusing ones from the previous tick.
//
// Between ticks it tracks which broker IDs were seen on the previous
// successful tick and deletes the metric label series for any broker ID that
// has since dropped out of the cluster, so a replaced broker doesn't leave
// behind a permanently-frozen, permanently-"stale" series.
func runConnectionHealthCheck(ctx context.Context, newProber func(ctx context.Context) (brokerConnectionProber, error), interval time.Duration, metrics *connectionProbeMetrics, logger *zap.Logger) {
	seenBrokerIDs := make(map[int32]struct{})

	if current := probeAllBrokersOnce(ctx, newProber, metrics, logger); current != nil {
		seenBrokerIDs = pruneStaleBrokerLabels(metrics, seenBrokerIDs, current)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := probeAllBrokersOnce(ctx, newProber, metrics, logger)
			if current == nil {
				// The tick failed before the current broker list could be
				// determined. Don't treat that as "no brokers" and delete
				// every series - keep the previously-seen set as-is.
				continue
			}
			seenBrokerIDs = pruneStaleBrokerLabels(metrics, seenBrokerIDs, current)
		}
	}
}

// pruneStaleBrokerLabels deletes the per-broker metric series for any broker
// ID present in previous but absent from current (i.e. brokers that have
// dropped out of the cluster since the last tick), and returns the set of
// currently-seen broker IDs for the caller to track as "previous" on the next
// call.
func pruneStaleBrokerLabels(metrics *connectionProbeMetrics, previous map[int32]struct{}, current []int32) map[int32]struct{} {
	currentSet := make(map[int32]struct{}, len(current))
	for _, id := range current {
		currentSet[id] = struct{}{}
	}

	for id := range previous {
		if _, stillPresent := currentSet[id]; stillPresent {
			continue
		}

		label := strconv.FormatInt(int64(id), 10)
		metrics.attemptsTotal.DeleteLabelValues(label)
		metrics.failuresTotal.DeleteLabelValues(label)
		metrics.lastSuccessTimestamp.DeleteLabelValues(label)
	}

	return currentSet
}

// probeAllBrokersOnce creates one prober, lists the current brokers, probes
// every broker concurrently with its own bounded timeout, records the result
// of each, and closes the prober before returning. It returns the broker IDs
// it just probed, or nil if it could not determine the cluster's current
// broker list (newProber or ListBrokerIDs failed), in which case
// tickFailuresTotal is incremented.
func probeAllBrokersOnce(ctx context.Context, newProber func(ctx context.Context) (brokerConnectionProber, error), metrics *connectionProbeMetrics, logger *zap.Logger) []int32 {
	prober, err := newProber(ctx)
	if err != nil {
		logger.Warn("connection health check: failed to create prober", zap.Error(err))
		metrics.tickFailuresTotal.Inc()
		return nil
	}
	defer prober.Close()

	brokerIDs, err := prober.ListBrokerIDs(ctx)
	if err != nil {
		logger.Warn("connection health check: failed to list brokers", zap.Error(err))
		metrics.tickFailuresTotal.Inc()
		return nil
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

	return brokerIDs
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
	// As of ApiVersions v3 (KIP-511), brokers reject the request with
	// INVALID_REQUEST unless ClientSoftwareName/Version are non-empty.
	// kgo.Client.Request fills these in automatically for *kmsg.ApiVersionsRequest,
	// but that convenience is specific to Client.Request; Broker.Request (used
	// here, deliberately, to probe one specific broker rather than "any" broker)
	// bypasses it, so we set them ourselves.
	req.ClientSoftwareName = "kminion"
	req.ClientSoftwareVersion = "connection-probe"
	resp, err := req.RequestWith(ctx, p.client.Broker(int(brokerID)))
	if err != nil {
		return fmt.Errorf("failed to request api versions from broker %d: %w", brokerID, err)
	}

	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return fmt.Errorf("broker %d returned an error for api versions request: %w", brokerID, err)
	}

	return nil
}

func (p *liveBrokerProber) Close() {
	p.client.Close()
}
