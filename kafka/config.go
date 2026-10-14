package kafka

import "fmt"

type Config struct {
	// General
	Brokers  []string `koanf:"brokers"`
	ClientID string   `koanf:"clientId"`
	RackID   string   `koanf:"rackId"`

	MaxConcurrentFetches int `koanf:"maxConcurrentFetches"`

	TLS  TLSConfig  `koanf:"tls"`
	SASL SASLConfig `koanf:"sasl"`

	RetryInitConnection bool `koanf:"retryInitConnection"`
}

func (c *Config) SetDefaults() {
	c.ClientID = "kminion"
	c.MaxConcurrentFetches = 10

	c.TLS.SetDefaults()
	c.SASL.SetDefaults()
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("no seed brokers specified, at least one must be configured")
	}

	if c.MaxConcurrentFetches < 1 {
		return fmt.Errorf("maxConcurrentFetches must be greater than 0")
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
