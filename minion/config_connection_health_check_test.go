package minion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionHealthCheckConfig_SetDefaults(t *testing.T) {
	c := ConnectionHealthCheckConfig{}
	c.SetDefaults()

	assert.False(t, c.Enabled)
	assert.Equal(t, time.Minute, c.ProbeInterval)
}

func TestConnectionHealthCheckConfig_Validate(t *testing.T) {
	tt := []struct {
		name    string
		cfg     ConnectionHealthCheckConfig
		wantErr bool
	}{
		{
			name:    "disabled with zero interval is valid",
			cfg:     ConnectionHealthCheckConfig{Enabled: false, ProbeInterval: 0},
			wantErr: false,
		},
		{
			name:    "enabled with zero interval is invalid",
			cfg:     ConnectionHealthCheckConfig{Enabled: true, ProbeInterval: 0},
			wantErr: true,
		},
		{
			name:    "enabled with negative interval is invalid",
			cfg:     ConnectionHealthCheckConfig{Enabled: true, ProbeInterval: -time.Second},
			wantErr: true,
		},
		{
			name:    "enabled with positive interval is valid",
			cfg:     ConnectionHealthCheckConfig{Enabled: true, ProbeInterval: time.Minute},
			wantErr: false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
