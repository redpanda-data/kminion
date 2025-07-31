package kafka

import "fmt"

const (
	SASLMechanismPlain       = "PLAIN"
	SASLMechanismScramSHA256 = "SCRAM-SHA-256"
	SASLMechanismScramSHA512 = "SCRAM-SHA-512"
	SASLMechanismGSSAPI      = "GSSAPI"
	SASLMechanismOAuthBearer = "OAUTHBEARER"
	SASLMechanismAWSMSKIAM   = "AWS_MSK_IAM"
)

// SASLConfig for Kafka Client
type SASLConfig struct {
	Enabled   bool   `koanf:"enabled"`
	Username  string `koanf:"username"`
	Password  string `koanf:"password"`
	Mechanism string `koanf:"mechanism"`

	// SASL Mechanisms that require more configuration than username & password
	GSSAPI      SASLGSSAPIConfig  `koanf:"gssapi"`
	OAuthBearer OAuthBearerConfig `koanf:"oauth"`
	AWS         AWS               `koanf:"aws"`
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
	case SASLMechanismPlain, SASLMechanismScramSHA256, SASLMechanismScramSHA512, SASLMechanismGSSAPI, SASLMechanismAWSMSKIAM:
		// Valid and supported
	case SASLMechanismOAuthBearer:
		return c.OAuthBearer.Validate()
	default:
		return fmt.Errorf("given sasl mechanism '%v' is invalid", c.Mechanism)
	}

	return nil
}
