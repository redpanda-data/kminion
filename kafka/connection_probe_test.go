package kafka

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeBrokerProber is a test double for brokerConnectionProber, so the probe
// loop's logic can be tested without a live Kafka cluster.
type fakeBrokerProber struct {
	brokerIDs   []int32
	listErr     error
	probeErrs   map[int32]error
	closeCalled bool
}

func (f *fakeBrokerProber) ListBrokerIDs(_ context.Context) ([]int32, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.brokerIDs, nil
}

func (f *fakeBrokerProber) ProbeBroker(_ context.Context, brokerID int32) error {
	return f.probeErrs[brokerID]
}

func (f *fakeBrokerProber) Close() {
	f.closeCalled = true
}

func TestProbeAllBrokersOnce_AllSucceed(t *testing.T) {
	fake := &fakeBrokerProber{
		brokerIDs: []int32{1, 2, 3},
		probeErrs: map[int32]error{},
	}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	for _, id := range []int32{1, 2, 3} {
		label := strconv.FormatInt(int64(id), 10)
		require.Equal(t, float64(1), testutil.ToFloat64(metrics.attemptsTotal.WithLabelValues(label)))
		require.Equal(t, float64(0), testutil.ToFloat64(metrics.failuresTotal.WithLabelValues(label)))
		require.Greater(t, testutil.ToFloat64(metrics.lastSuccessTimestamp.WithLabelValues(label)), float64(0))
	}
	require.True(t, fake.closeCalled)
}

func TestProbeAllBrokersOnce_OneBrokerFailsIndependently(t *testing.T) {
	fake := &fakeBrokerProber{
		brokerIDs: []int32{1, 2},
		probeErrs: map[int32]error{2: errors.New("connection refused")},
	}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.Equal(t, float64(0), testutil.ToFloat64(metrics.failuresTotal.WithLabelValues("1")))
	require.Greater(t, testutil.ToFloat64(metrics.lastSuccessTimestamp.WithLabelValues("1")), float64(0))

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.failuresTotal.WithLabelValues("2")))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.lastSuccessTimestamp.WithLabelValues("2")))
}

func TestProbeAllBrokersOnce_ListBrokersFails(t *testing.T) {
	fake := &fakeBrokerProber{listErr: errors.New("metadata request failed")}
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return fake, nil }

	probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.True(t, fake.closeCalled)
	require.Equal(t, 0, testutil.CollectAndCount(metrics.attemptsTotal), "no per-broker metric should exist when listing brokers fails")
}

func TestProbeAllBrokersOnce_NewProberFails(t *testing.T) {
	metrics := newConnectionProbeMetrics(prometheus.NewRegistry())
	newProber := func(_ context.Context) (brokerConnectionProber, error) { return nil, errors.New("dial failed") }

	// Must not panic even though no prober could be created.
	probeAllBrokersOnce(context.Background(), newProber, metrics, zap.NewNop())

	require.Equal(t, 0, testutil.CollectAndCount(metrics.attemptsTotal))
}
