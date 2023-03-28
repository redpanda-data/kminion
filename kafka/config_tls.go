package kafka

import "fmt"

// TLSConfig to connect to Kafka via TLS
type TLSConfig struct {
	Enabled               bool   `koanf:"enabled"`
	CaFilepath            string `koanf:"caFilepath"`
	CertFilepath          string `koanf:"certFilepath"`
	KeyFilepath           string `koanf:"keyFilepath"`
	Ca                    string `koanf:"ca"`
	Cert                  string `koanf:"cert"`
	Key                   string `koanf:"key"`
	Passphrase            string `koanf:"passphrase"`
	InsecureSkipTLSVerify bool   `koanf:"insecureSkipTlsVerify"`
}

func (c *TLSConfig) SetDefaults() {
	c.Enabled = false
}

func (c *TLSConfig) Validate() error {
	if len(c.CaFilepath) > 0 && len(c.Ca) > 0 {
		return fmt.Errorf("config keys 'caFilepath' and 'ca' are both set. only one can be used at the same time")
	}
	if len(c.CertFilepath) > 0 && len(c.Cert) > 0 {
		return fmt.Errorf("config keys 'certFilepath' and 'cert' are both set. only one can be used at the same time")
	}

	if len(c.KeyFilepath) > 0 && len(c.Key) > 0 {
		return fmt.Errorf("config keys 'keyFilepath' and 'key' are both set. only one can be used at the same time")
	}
	return nil
}
