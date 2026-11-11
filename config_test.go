package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewConfig_EnvValues(t *testing.T) {
	tests := []struct {
		name         string
		brokers      string
		password     string
		wantBrokers  []string
		wantPassword string
	}{
		{
			name:         "single broker",
			brokers:      "a:9092",
			password:     "secret",
			wantBrokers:  []string{"a:9092"},
			wantPassword: "secret",
		},
		{
			name:         "comma-separated brokers and comma in password",
			brokers:      "a:9092,b:9092",
			password:     "dummyA,dummyB",
			wantBrokers:  []string{"a:9092", "b:9092"},
			wantPassword: "dummyA,dummyB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CONFIG_FILEPATH", "")
			t.Setenv("KAFKA_BROKERS", tt.brokers)
			t.Setenv("KAFKA_SASL_ENABLED", "true")
			t.Setenv("KAFKA_SASL_USERNAME", "user")
			t.Setenv("KAFKA_SASL_PASSWORD", tt.password)

			cfg, err := newConfig(zap.NewNop())
			require.NoError(t, err)
			require.Equal(t, tt.wantBrokers, cfg.Kafka.Brokers)
			require.Equal(t, tt.wantPassword, cfg.Kafka.SASL.Password)
		})
	}
}

func TestNewConfig_IgnoresServiceLinkEnvVars(t *testing.T) {
	// Each name is a Service in the pod's namespace that maps onto a config key.
	services := []string{"EXPORTER", "KAFKA_BROKERS", "LOGGER_LEVEL", "EXPORTER_HOST", "KAFKA_CLIENTID", "KAFKA_TLS_ENABLED", "MINION_ENDTOEND_ENABLED"}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("kafka:\n  brokers: [\"a:9092\"]\n"), 0o600))
	for _, svc := range services {
		t.Run(svc, func(t *testing.T) {
			t.Setenv("CONFIG_FILEPATH", configPath)
			// The env vars that the kubelet injects for a Service with a port named "metrics" on 8080.
			t.Setenv(svc+"_SERVICE_HOST", "10.0.0.1")
			t.Setenv(svc+"_SERVICE_PORT", "8080")
			t.Setenv(svc+"_SERVICE_PORT_METRICS", "8080")
			t.Setenv(svc+"_PORT", "tcp://10.0.0.1:8080")
			t.Setenv(svc+"_PORT_8080_TCP", "tcp://10.0.0.1:8080")
			t.Setenv(svc+"_PORT_8080_TCP_PROTO", "tcp")
			t.Setenv(svc+"_PORT_8080_TCP_PORT", "8080")
			t.Setenv(svc+"_PORT_8080_TCP_ADDR", "10.0.0.1")

			cfg, err := newConfig(zap.NewNop())
			require.NoError(t, err)
			require.Equal(t, []string{"a:9092"}, cfg.Kafka.Brokers)
			require.Equal(t, 8080, cfg.Exporter.Port)
		})
	}
}

func TestNewConfig_ExporterPortEnvVar(t *testing.T) {
	t.Setenv("CONFIG_FILEPATH", "")
	t.Setenv("KAFKA_BROKERS", "a:9092")
	t.Setenv("EXPORTER_PORT", "9000")

	cfg, err := newConfig(zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, 9000, cfg.Exporter.Port)
}

func TestIsServiceLinkEnvVar(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "KAFKA_BROKERS_SERVICE_HOST", value: "10.0.0.1", want: true},
		{name: "KAFKA_BROKERS_SERVICE_PORT", value: "9092", want: true},
		{name: "KAFKA_BROKERS_SERVICE_PORT_KAFKA_TLS", value: "9093", want: true},
		{name: "EXPORTER_PORT", value: "tcp://10.0.0.1:8080", want: true},
		{name: "EXPORTER_PORT", value: "udp://10.0.0.1:8080", want: true},
		{name: "EXPORTER_PORT_8080_TCP", value: "tcp://10.0.0.1:8080", want: true},
		{name: "EXPORTER_PORT_8080_SCTP_ADDR", value: "10.0.0.1", want: true},
		{name: "EXPORTER_PORT", value: "8080", want: false},
		{name: "KAFKA_BROKERS", value: "a:9092", want: false},
		{name: "KAFKA_SASL_GSSAPI_SERVICENAME", value: "kafka", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name+"="+tt.value, func(t *testing.T) {
			require.Equal(t, tt.want, isServiceLinkEnvVar(tt.name, tt.value))
		})
	}
}
