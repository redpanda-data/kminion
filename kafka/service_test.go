package kafka

import (
	"context"
	"testing"

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
