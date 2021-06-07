package minion

import (
	"fmt"

	"github.com/cloudhut/kminion/v2/e2e"
)

type Config struct {
	ConsumerGroups ConsumerGroupConfig `koanf:"consumerGroups"`
	Topics         TopicConfig         `koanf:"topics"`
	LogDirs        LogDirsConfig       `koanf:"logDirs"`
	EndToEnd       e2e.Config          `koanf:"endToEnd"`
}

func (c *Config) SetDefaults() {
	c.ConsumerGroups.SetDefaults()
	c.Topics.SetDefaults()
	c.LogDirs.SetDefaults()
	c.EndToEnd.SetDefaults()
}

func (c *Config) Validate() error {
	err := c.ConsumerGroups.Validate()
	if err != nil {
		return fmt.Errorf("failed to consumer group config: %w", err)
	}

	err = c.Topics.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate topic config: %w", err)
	}

	err = c.LogDirs.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate log dirs config: %w", err)
	}

	err = c.EndToEnd.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate endToEnd config: %w", err)
	}

	return nil
}
