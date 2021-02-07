package kafka

// TLSConfig to connect to Kafka via TLS
type TLSConfig struct {
	Enabled               bool   `koanf:"enabled"`
	CaFilepath            string `koanf:"caFilepath"`
	CertFilepath          string `koanf:"certFilepath"`
	KeyFilepath           string `koanf:"keyFilepath"`
	Passphrase            string `koanf:"passphrase"`
	InsecureSkipTLSVerify bool   `koanf:"insecureSkipTlsVerify"`
}

func (c *TLSConfig) SetDefaults() {
	c.Enabled = false
}

func (c *TLSConfig) Validate() error {
	return nil
}
