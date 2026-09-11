package kafka

import (
	"fmt"
	"time"
)

// ConnectionHealthCheckConfig configures an optional periodic probe that opens
// a brand new connection to every broker in the cluster to detect a broker
// that has silently stopped accepting new Kafka connections, while kminion's
// own long-lived clients (used by the minion and end-to-end services) keep
// working on connections they already established.
//
// This is disabled by default: existing kminion deployments that don't set
// this key see no behavior change.
type ConnectionHealthCheckConfig struct {
	// Enabled turns the probe on.
	Enabled bool `koanf:"enabled"`
	// Interval is how often the probe runs against every broker in the
	// cluster. Values shorter than the ~10s per-broker request timeout
	// (connectionProbeRequestTimeout) are allowed but not particularly
	// useful, since a tick runs to completion before the next one starts
	// and so ticks never overlap.
	Interval time.Duration `koanf:"interval"`
}

func (c *ConnectionHealthCheckConfig) SetDefaults() {
	c.Enabled = false
	c.Interval = time.Minute
}

func (c *ConnectionHealthCheckConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Interval <= 0 {
		return fmt.Errorf("failed to validate connectionHealthCheck config: interval must be greater than zero")
	}

	return nil
}
