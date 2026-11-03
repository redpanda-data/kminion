package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndToEndConsumerConfig_SetDefaults(t *testing.T) {
	var cfg EndToEndConsumerConfig
	cfg.SetDefaults()

	assert.Equal(t, 30*time.Second, cfg.InitTimeout)
	require.NoError(t, cfg.Validate())
}

func TestEndToEndConsumerConfig_ValidateInitTimeout(t *testing.T) {
	tests := []struct {
		name        string
		initTimeout time.Duration
		wantErr     bool
	}{
		{name: "default", initTimeout: 30 * time.Second},
		{name: "longer than the default", initTimeout: 3 * time.Minute},
		{name: "zero", initTimeout: 0, wantErr: true},
		{name: "negative", initTimeout: -time.Second, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg EndToEndConsumerConfig
			cfg.SetDefaults()
			cfg.InitTimeout = tt.initTimeout

			err := cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "initTimeout")
				return
			}
			require.NoError(t, err)
		})
	}
}
