package minion

import (
	"fmt"
	"time"
)

type EndToEndProducerConfig struct {
	LatencySla time.Duration `koanf:"latencySla"`
}

func (c *EndToEndProducerConfig) Validate() error {
	_, err := time.ParseDuration(c.LatencySla.String())
	if err != nil {
		return fmt.Errorf("failed to parse '%s' to time.Duration: %v", c.LatencySla.String(), err)
	}
	return nil
}
