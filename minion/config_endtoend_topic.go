package minion

import (
	"fmt"
	"time"
)

type EndToEndTopicConfig struct {
	Enabled               bool          `koanf:"enabled"`
	Name                  string        `koanf:"name"`
	ReplicationFactor     int           `koanf:"replicationFactor"`
	PartitionsPerBroker   int           `koanf:"partitionsPerBroker"`
	ReconcilationInterval time.Duration `koanf:"reconcilationInterval"`
}

func (c *EndToEndTopicConfig) SetDefaults() {
	c.Enabled = true
	c.Name = "kminion-end-to-end"
}

func (c *EndToEndTopicConfig) Validate() error {

	_, err := time.ParseDuration(c.ReconcilationInterval.String())
	if err != nil {
		return fmt.Errorf("failed to parse '%s' to time.Duration: %v", c.ReconcilationInterval.String(), err)
	}

	return nil
}
