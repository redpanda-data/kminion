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
}

func (c *EndToEndConfig) Validate() error {
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
