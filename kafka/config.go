package kafka

import "fmt"

type Config struct {
	// General
	Brokers  []string `koanf:"brokers"`
	ClientID string   `koanf:"clientId"`
	RackID   string   `koanf:"rackId"`

	TLS  TLSConfig  `koanf:"tls"`
	SASL SASLConfig `koanf:"sasl"`
}

func (c *Config) SetDefaults() {
	c.ClientID = "kminion"

	c.TLS.SetDefaults()
	c.SASL.SetDefaults()
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("no seed brokers specified, at least one must be configured")
	}

	err := c.TLS.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate TLS config: %w", err)
	}

	err = c.SASL.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate SASL config: %w", err)
	}

	return nil
}
