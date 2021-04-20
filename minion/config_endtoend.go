package minion

import (
	"fmt"
	"time"
)

type EndToEndConfig struct {
	Enabled         bool                   `koanf:"enabled"`
	TopicManagement EndToEndTopicConfig    `koanf:"topicManagement"`
	ProbeInterval   time.Duration          `koanf:"probeInterval"`
	Producer        EndToEndProducerConfig `koanf:"producer"`
	Consumer        EndToEndConsumerConfig `koanf:"consumer"`
}

func (c *EndToEndConfig) SetDefaults() {
	c.Enabled = false
	c.ProbeInterval = 2 * time.Second
	c.TopicManagement.SetDefaults()
	c.Producer.SetDefaults()
	c.Consumer.SetDefaults()
}

func (c *EndToEndConfig) Validate() error {

	if !c.Enabled {
		return nil
	}

	// If the timeduration is 0s or 0ms or its variation of zero, it will be parsed as 0
	if c.ProbeInterval == 0 {
		return fmt.Errorf("failed to validate probeInterval config, the duration can't be zero")
	}

	err := c.TopicManagement.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate topicManagement config: %w", err)
	}

	_, err = time.ParseDuration(c.ProbeInterval.String())
	if err != nil {
		return fmt.Errorf("failed to parse '%s' to time.Duration: %v", c.ProbeInterval.String(), err)
	}

	err = c.Producer.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate producer config: %w", err)
	}

	err = c.Consumer.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate consumer config: %w", err)
	}

	return nil
}
