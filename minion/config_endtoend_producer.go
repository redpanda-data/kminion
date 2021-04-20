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
	c.LatencySla = 5 * time.Second
	c.RequiredAcks = -1
}

func (c *EndToEndProducerConfig) Validate() error {

	// If the timeduration is 0s or 0ms or its variation of zero, it will be parsed as 0
	if c.LatencySla == 0 {
		return fmt.Errorf("failed to validate producer.latencySla config, the duration can't be zero")
	}

	switch c.RequiredAcks {
	// Only allows -1 All ISR Ack, idempotence EOS on producing message
	// or 1 where the Leader Ack is neede, the rest should return error
	case -1, 1:
	default:
		return fmt.Errorf("failed to parse producer.RequiredAcks, valid value is either -1, 1")
	}

	return nil
}
