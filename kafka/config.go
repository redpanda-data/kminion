package kafka

import (
	"fmt"
	"io/ioutil"
)

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

// InMemoryConfig is config that is suitable to be used when kminion is used as
// library because it accept tls certs as byte arrays, not as file paths
type InMemoryConfig struct {
	// General
	Brokers  []string
	ClientID string
	RackID   string

	TLS  InMemoryTLSConfig
	SASL SASLConfig
}

func (c *InMemoryConfig) SetDefaults() {
	c.ClientID = "kminion"

	c.TLS.SetDefaults()
	c.SASL.SetDefaults()
}

func (c *InMemoryConfig) Validate() error {
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

func FromConfig(cfg Config) (InMemoryConfig, error) {
	result := InMemoryConfig{
		Brokers:  cfg.Brokers,
		ClientID: cfg.ClientID,
		RackID:   cfg.RackID,
		SASL:     cfg.SASL,
		TLS: InMemoryTLSConfig{
			Enabled:               cfg.TLS.Enabled,
			Passphrase:            cfg.TLS.Passphrase,
			InsecureSkipTLSVerify: cfg.TLS.InsecureSkipTLSVerify,
		},
	}
	if cfg.TLS.CaFilepath != "" {
		ca, err := ioutil.ReadFile(cfg.TLS.CaFilepath)
		if err != nil {
			return result, err
		}
		result.TLS.Ca = ca
	}
	if cfg.TLS.CertFilepath != "" {
		cert, err := ioutil.ReadFile(cfg.TLS.CertFilepath)
		if err != nil {
			return result, err
		}
		result.TLS.Cert = cert
	}
	if cfg.TLS.KeyFilepath != "" {
		key, err := ioutil.ReadFile(cfg.TLS.KeyFilepath)
		if err != nil {
			return result, err
		}
		result.TLS.Key = key
	}
	return result, nil
}
