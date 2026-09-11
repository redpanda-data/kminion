package kafka

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeBrokerProber is a test double for brokerConnectionProber, so the probe
// loop's logic can be tested without a live Kafka cluster.
type fakeBrokerProber struct {
	mu sync.Mutex

	brokerIDs []int32
	listErr   error
	probeErrs map[int32]error

	// listBrokerIDsSequence, if non-nil, overrides brokerIDs/listErr: each
	// call to ListBrokerIDs returns the next entry, clamping to the last
	// entry once the sequence is exhausted. This lets tests simulate the
	// broker list changing across ticks.
	listBrokerIDsSequence [][]int32

	listCalls   int
	closeCalled bool
}

func (f *fakeBrokerProber) ListBrokerIDs(_ context.Context) ([]int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listCalls++

	if f.listBrokerIDsSequence != nil {
		idx := f.listCalls - 1
		if idx >= len(f.listBrokerIDsSequence) {
			idx = len(f.listBrokerIDsSequence) - 1
		}
		return f.listBrokerIDsSequence[idx], nil
	}

	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.brokerIDs, nil
}

func (f *fakeBrokerProber) ProbeBroker(_ context.Context, brokerID int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probeErrs[brokerID]
}

func (f *fakeBrokerProber) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalled = true
}

func (f *fakeBrokerProber) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

func TestProbeAllBrokersOnce_AllSucceed(t *testing.T) {
	fake := &fakeBrokerProber{
		brokerIDs: []int32{1, 2, 3},
		probeErrs: map[int32]error{},
	}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	ids := probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.ElementsMatch(t, []int32{1, 2, 3}, ids)
	for _, id := range []int32{1, 2, 3} {
		label := strconv.FormatInt(int64(id), 10)
		require.Equal(t, float64(1), testutil.ToFloat64(metrics.attemptsTotal.WithLabelValues(label)))
		require.Equal(t, float64(0), testutil.ToFloat64(metrics.failuresTotal.WithLabelValues(label)))
		require.Greater(t, testutil.ToFloat64(metrics.lastSuccessTimestamp.WithLabelValues(label)), float64(0))
	}
	require.True(t, fake.closeCalled)
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.tickFailuresTotal), "a successful tick must not count as a tick failure")
}

func TestProbeAllBrokersOnce_OneBrokerFailsIndependently(t *testing.T) {
	fake := &fakeBrokerProber{
		brokerIDs: []int32{1, 2},
		probeErrs: map[int32]error{2: errors.New("connection refused")},
	}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	ids := probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.ElementsMatch(t, []int32{1, 2}, ids)

	require.Equal(t, float64(0), testutil.ToFloat64(metrics.failuresTotal.WithLabelValues("1")))
	require.Greater(t, testutil.ToFloat64(metrics.lastSuccessTimestamp.WithLabelValues("1")), float64(0))

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.failuresTotal.WithLabelValues("2")))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.lastSuccessTimestamp.WithLabelValues("2")))
}

func TestProbeAllBrokersOnce_ListBrokersFails(t *testing.T) {
	fake := &fakeBrokerProber{listErr: errors.New("metadata request failed")}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	ids := probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.Nil(t, ids)
	require.True(t, fake.closeCalled)
	require.Equal(t, 0, testutil.CollectAndCount(metrics.attemptsTotal), "no per-broker metric should exist when listing brokers fails")
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.tickFailuresTotal))
}

func TestProbeAllBrokersOnce_NewProberFails(t *testing.T) {
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return nil, errors.New("dial failed") }

	// Must not panic even though no prober could be created.
	ids := probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.Nil(t, ids)
	require.Equal(t, 0, testutil.CollectAndCount(metrics.attemptsTotal))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.tickFailuresTotal))
}

func TestProbeAllBrokersOnce_ReturnsCurrentBrokerIDs(t *testing.T) {
	testCases := []struct {
		name      string
		fake      *fakeBrokerProber
		newProber func(fake *fakeBrokerProber) func(context.Context) (brokerConnectionProber, error)
		wantIDs   []int32
	}{
		{
			name: "success returns the probed broker IDs",
			fake: &fakeBrokerProber{brokerIDs: []int32{5, 6}, probeErrs: map[int32]error{}},
			newProber: func(fake *fakeBrokerProber) func(context.Context) (brokerConnectionProber, error) {
				return func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }
			},
			wantIDs: []int32{5, 6},
		},
		{
			name: "ListBrokerIDs failure returns nil",
			fake: &fakeBrokerProber{listErr: errors.New("boom")},
			newProber: func(fake *fakeBrokerProber) func(context.Context) (brokerConnectionProber, error) {
				return func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }
			},
			wantIDs: nil,
		},
		{
			name: "newProber failure returns nil",
			fake: &fakeBrokerProber{},
			newProber: func(_ *fakeBrokerProber) func(context.Context) (brokerConnectionProber, error) {
				return func(_ context.Context) (brokerConnectionProber, error) { return nil, errors.New("dial failed") }
			},
			wantIDs: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
			ids := probeAllBrokersOnce(context.Background(), tc.newProber(tc.fake), metrics, zap.NewNop())
			if tc.wantIDs == nil {
				require.Nil(t, ids)
			} else {
				require.ElementsMatch(t, tc.wantIDs, ids)
			}
		})
	}
}

func TestNewConnectionProbeMetrics_ExactMetricNames(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newConnectionProbeMetrics(registry)

	// CounterVec/GaugeVec collectors report no metric family at all until at
	// least one label combination has been observed, so record one success
	// and one failure to populate all three per-broker series before
	// gathering. tickFailuresTotal is a plain Counter and is always present.
	recordProbeResult(metrics, 1, nil, time.Now())
	recordProbeResult(metrics, 2, errors.New("boom"), time.Now())

	families, err := registry.Gather()
	require.NoError(t, err)

	var names []string
	for _, f := range families {
		names = append(names, f.GetName())
	}

	require.ElementsMatch(t, []string{
		"kafka_connection_probe_attempts_total",
		"kafka_connection_probe_failures_total",
		"kafka_connection_probe_last_success_timestamp_seconds",
		"kafka_connection_probe_tick_failures_total",
	}, names)
}

func TestPruneStaleBrokerLabels_DeletesDroppedBrokerSeries(t *testing.T) {
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	recordProbeResult(metrics, 1, nil, time.Now())
	recordProbeResult(metrics, 2, nil, time.Now())

	previous := map[int32]struct{}{1: {}, 2: {}}
	current := pruneStaleBrokerLabels(metrics, previous, []int32{1})

	require.Equal(t, map[int32]struct{}{1: {}}, current)
	require.Equal(t, 1, testutil.CollectAndCount(metrics.attemptsTotal), "broker 2's attempts series should have been deleted")
	require.Equal(t, 1, testutil.CollectAndCount(metrics.lastSuccessTimestamp), "broker 2's last-success series should have been deleted")

	// Broker 1's series must survive untouched.
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.attemptsTotal.WithLabelValues("1")))
}

func TestRunConnectionHealthCheck_TicksRepeatedlyUntilCanceled(t *testing.T) {
	fake := &fakeBrokerProber{brokerIDs: []int32{1}, probeErrs: map[int32]error{}}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		runConnectionHealthCheck(ctx, newProber, 5*time.Millisecond, metrics, zap.NewNop())
		close(done)
	}()

	require.Eventually(t, func() bool {
		return fake.callCount() >= 2
	}, time.Second, time.Millisecond, "expected at least two ticks before cancellation")

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runConnectionHealthCheck did not return promptly after ctx was canceled")
	}
}

func TestRunConnectionHealthCheck_PrunesStaleBrokerLabels(t *testing.T) {
	fake := &fakeBrokerProber{
		probeErrs:             map[int32]error{},
		listBrokerIDsSequence: [][]int32{{1, 2}, {1}},
	}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		runConnectionHealthCheck(ctx, newProber, 5*time.Millisecond, metrics, zap.NewNop())
		close(done)
	}()

	// The first call happens immediately (before the ticker even fires) and
	// returns brokers [1, 2]; the second call (on the first tick) returns
	// just [1], so broker 2 must be dropped by then.
	require.Eventually(t, func() bool {
		return fake.callCount() >= 2
	}, time.Second, time.Millisecond, "expected at least two probe calls")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runConnectionHealthCheck did not return promptly after ctx was canceled")
	}

	require.Equal(t, 1, testutil.CollectAndCount(metrics.attemptsTotal), "broker 2's series should have been pruned once it dropped out")
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.attemptsTotal.WithLabelValues("2")), "a deleted series reports as 0, not its old frozen value")
}
