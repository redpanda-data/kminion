package minion

import (
	"fmt"
	"time"
)

type EndToEndProducerConfig struct {
	LatencySla   time.Duration `koanf:"latencySla"`
	RequiredAcks int           `koanf:"requiredAcks"`
}

func (c *EndToEndProducerConfig) SetDefaults() {
	latency, _ := time.ParseDuration("5s")
	c.LatencySla = latency
	c.RequiredAcks = -1
}

func (c *EndToEndProducerConfig) Validate() error {
	_, err := time.ParseDuration(c.LatencySla.String())
	if err != nil {
		return fmt.Errorf("failed to parse '%s' to time.Duration: %v", c.LatencySla.String(), err)
	}

	if c.RequiredAcks < -1 || c.RequiredAcks > 1 {
		return fmt.Errorf("failed to parse producer.RequiredAcks, valid value is either -1, 0, 1")
	}
	return nil
}
