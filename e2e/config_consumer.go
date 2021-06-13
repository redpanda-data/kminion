package e2e

import (
	"fmt"
	"time"
)

type EndToEndConsumerConfig struct {
	GroupIdPrefix             string `koanf:"groupIdPrefix"`
	DeleteStaleConsumerGroups bool   `koanf:"deleteStaleConsumerGroups"`

	RoundtripSla time.Duration `koanf:"roundtripSla"`
	CommitSla    time.Duration `koanf:"commitSla"`
}

func (c *EndToEndConsumerConfig) SetDefaults() {
	c.GroupIdPrefix = "kminion-end-to-end"
	c.DeleteStaleConsumerGroups = false
	c.RoundtripSla = 20 * time.Second
	c.CommitSla = 10 * time.Second // no idea what to use as a good default value
}

func (c *EndToEndConsumerConfig) Validate() error {
	if len(c.GroupIdPrefix) < 3 {
		return fmt.Errorf("kminion prefix should be at least 3 characters long")
	}

	if c.RoundtripSla <= 0 {
		return fmt.Errorf("consumer.roundtripSla must be greater than zero")
	}

	if c.CommitSla <= 0 {
		return fmt.Errorf("consumer.commitSla must be greater than zero")
	}

	return nil
}
