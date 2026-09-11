package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestService_StartConnectionHealthCheck_DisabledIsNoop(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	cfg.Brokers = []string{"localhost:9092"}
	// cfg.ConnectionHealthCheck.Enabled defaults to false via SetDefaults().

	svc := NewService(cfg, zap.NewNop())
	registry := prometheus.NewRegistry()

	svc.StartConnectionHealthCheck(context.Background(), registry)

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, metricFamilies, "no metrics should be registered when connectionHealthCheck is disabled")
}

func TestService_StartConnectionHealthCheck_EnabledRegistersMetrics(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	// No live cluster is required: the point of this test is only to confirm
	// that enabling the check registers its metrics and starts the
	// background loop, not to successfully probe a broker.
	cfg.Brokers = []string{"127.0.0.1:1"}
	cfg.ConnectionHealthCheck.Enabled = true
	cfg.ConnectionHealthCheck.Interval = time.Hour

	svc := NewService(cfg, zap.NewNop())
	registry := prometheus.NewRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.StartConnectionHealthCheck(ctx, registry)

	// StartConnectionHealthCheck registers the metrics synchronously, before
	// launching the background loop in a goroutine, so this can be asserted
	// immediately without waiting for a tick.
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies, "metrics should be registered as soon as the check is enabled")
}
