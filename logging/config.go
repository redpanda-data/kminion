package logging

import (
	"fmt"
	"go.uber.org/zap"
)

type Config struct {
	Level string `koanf:"level"`
}

func (c *Config) SetDefaults() {
	c.Level = "info"
}

func (c *Config) Validate() error {
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(c.Level))
	if err != nil {
		return fmt.Errorf("failed to parse logger level: %w", err)
	}

	return nil
}
