package main

import (
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
