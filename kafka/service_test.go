package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestService_StartConnectionHealthCheck_DisabledIsNoop(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	cfg.Brokers = []string{"localhost:9092"}

	svc := NewService(cfg, zap.NewNop())
	registry := prometheus.NewRegistry()

	svc.StartConnectionHealthCheck(context.Background(), false, time.Minute, registry)

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

	svc := NewService(cfg, zap.NewNop())
	registry := prometheus.NewRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.StartConnectionHealthCheck(ctx, true, time.Hour, registry)

	// StartConnectionHealthCheck registers the metrics synchronously, before
	// launching the background loop in a goroutine, so this can be asserted
	// immediately without waiting for a tick.
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies, "metrics should be registered as soon as the check is enabled")
}

func TestService_CreateAndTestClient_RetryStopsOnContextCancel(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	// Nothing listens on this port, so every connection test fails.
	cfg.Brokers = []string{"127.0.0.1:1"}
	cfg.RetryInitConnection = true

	svc := NewService(cfg, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	client, err := svc.CreateAndTestClient(ctx, zap.NewNop(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, client)
	require.Less(t, time.Since(start), 5*time.Second, "the retry loop should stop when the context is done")
}

func TestNewService_OAuthBearerTokenEndpointScheme(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantWarn bool
	}{
		{name: "https", endpoint: "https://auth.example.com/token", wantWarn: false},
		{name: "uppercase https", endpoint: "HTTPS://auth.example.com/token", wantWarn: false},
		{name: "http", endpoint: "http://auth.example.com/token", wantWarn: true},
		{name: "no scheme", endpoint: "auth.example.com/token", wantWarn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{}
			cfg.SetDefaults()
			cfg.Brokers = []string{"localhost:9092"}
			cfg.SASL.Enabled = true
			cfg.SASL.Mechanism = SASLMechanismOAuthBearer
			cfg.SASL.OAuthBearer.TokenEndpoint = tt.endpoint
			cfg.SASL.OAuthBearer.ClientID = "client"
			cfg.SASL.OAuthBearer.ClientSecret = "dummy"
			require.NoError(t, cfg.Validate())

			core, logs := observer.New(zap.WarnLevel)
			NewService(cfg, zap.New(core))
			require.Equal(t, tt.wantWarn, logs.FilterMessageSnippet("does not use https").Len() > 0)
		})
	}
}

func TestNewService_SASLWithoutTLS(t *testing.T) {
	tests := []struct {
		mechanism  string
		tlsEnabled bool
		wantWarn   bool
	}{
		{mechanism: SASLMechanismPlain, tlsEnabled: false, wantWarn: true},
		{mechanism: SASLMechanismPlain, tlsEnabled: true, wantWarn: false},
		{mechanism: SASLMechanismOAuthBearer, tlsEnabled: false, wantWarn: true},
		{mechanism: SASLMechanismOAuthBearer, tlsEnabled: true, wantWarn: false},
		// SCRAM and GSSAPI do not send the password.
		{mechanism: SASLMechanismScramSHA512, tlsEnabled: false, wantWarn: false},
		{mechanism: SASLMechanismGSSAPI, tlsEnabled: false, wantWarn: false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/tls=%v", tt.mechanism, tt.tlsEnabled), func(t *testing.T) {
			cfg := Config{}
			cfg.SetDefaults()
			cfg.Brokers = []string{"localhost:9092"}
			cfg.SASL.Enabled = true
			cfg.SASL.Mechanism = tt.mechanism
			cfg.SASL.OAuthBearer.TokenEndpoint = "https://auth.example.com/token"
			cfg.TLS.Enabled = tt.tlsEnabled

			core, logs := observer.New(zap.WarnLevel)
			NewService(cfg, zap.New(core))
			require.Equal(t, tt.wantWarn, logs.FilterMessageSnippet("without TLS").Len() == 1)
		})
	}
}
