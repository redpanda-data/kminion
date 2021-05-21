package e2e

import (
	"fmt"
	"time"
)

type EndToEndProducerConfig struct {
	AckSla       time.Duration `koanf:"ackSla"`
	RequiredAcks string        `koanf:"requiredAcks"`
}

func (c *EndToEndProducerConfig) SetDefaults() {
	c.AckSla = 5 * time.Second
	c.RequiredAcks = "all"
}

func (c *EndToEndProducerConfig) Validate() error {

	if c.RequiredAcks != "all" && c.RequiredAcks != "leader" {
		return fmt.Errorf("producer.requiredAcks must be 'all' or 'leader")
	}

	if c.AckSla <= 0 {
		return fmt.Errorf("producer.ackSla must be greater than zero")
	}

	return nil
}
