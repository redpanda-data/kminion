package kafka

import "fmt"

const (
	SASLMechanismPlain       = "PLAIN"
	SASLMechanismScramSHA256 = "SCRAM-SHA-256"
	SASLMechanismScramSHA512 = "SCRAM-SHA-512"
	SASLMechanismGSSAPI      = "GSSAPI"
	SASLMechanismOAuthBearer = "OAUTHBEARER"
)

// SASLConfig for Kafka Client
type SASLConfig struct {
	Enabled   bool   `koanf:"enabled"`
	Username  string `koanf:"username"`
	Password  string `koanf:"password"`
	Mechanism string `koanf:"mechanism"`

	// SASL Mechanisms that require more configuration than username & password
	GSSAPI SASLGSSAPIConfig `koanf:"gssapi"`
}

// SetDefaults for SASL Config
func (c *SASLConfig) SetDefaults() {
	c.Enabled = false
	c.Mechanism = SASLMechanismPlain
	c.GSSAPI.SetDefaults()
}

// Validate SASL config input
func (c *SASLConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	switch c.Mechanism {
	case SASLMechanismPlain, SASLMechanismScramSHA256, SASLMechanismScramSHA512, SASLMechanismGSSAPI:
		// Valid and supported
	case SASLMechanismOAuthBearer:
		return fmt.Errorf("sasl mechanism '%v' is valid but not yet supported. Please submit an issue if you need it", c.Mechanism)
	default:
		return fmt.Errorf("given sasl mechanism '%v' is invalid", c.Mechanism)
	}

	return nil
}
