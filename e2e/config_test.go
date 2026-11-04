package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name          string
		probeInterval time.Duration
		wantErr       bool
	}{
		{name: "default probe interval", probeInterval: 100 * time.Millisecond, wantErr: false},
		{name: "zero probe interval", probeInterval: 0, wantErr: true},
		{name: "negative probe interval", probeInterval: -time.Second, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			cfg.SetDefaults()
			cfg.Enabled = true
			cfg.ProbeInterval = tt.probeInterval
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
