package e2e

import (
	"fmt"
	"time"
)

type EndToEndProducerConfig struct {
	AckSla       time.Duration `koanf:"ackSla"`
	RequiredAcks int           `koanf:"requiredAcks"`
}

func (c *EndToEndProducerConfig) SetDefaults() {
	c.AckSla = 5 * time.Second
	c.RequiredAcks = -1
}

func (c *EndToEndProducerConfig) Validate() error {

	if c.AckSla <= 0 {
		return fmt.Errorf("producer.ackSla must be greater than zero")
	}

	// all(-1) or leader(1)
	if c.RequiredAcks != -1 && c.RequiredAcks != 1 {
		return fmt.Errorf("producer.requiredAcks must be 1 or -1")
	}

	return nil
}
