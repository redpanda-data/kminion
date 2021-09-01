package kafka

// SASLGSSAPIConfig represents the Kafka Kerberos config
type SASLGSSAPIConfig struct {
	AuthType           string `koanf:"authType"`
	KeyTabPath         string `koanf:"keyTabPath"`
	KerberosConfigPath string `koanf:"kerberosConfigPath"`
	ServiceName        string `koanf:"serviceName"`
	Username           string `koanf:"username"`
	Password           string `koanf:"password"`
	Realm              string `koanf:"realm"`

	// EnableFAST enables FAST, which is a pre-authentication framework for Kerberos.
	// It includes a mechanism for tunneling pre-authentication exchanges using armoured KDC messages.
	// FAST provides increased resistance to passive password guessing attacks.
	EnableFast bool `koanf:"enableFast"`
}

func (s *SASLGSSAPIConfig) SetDefaults() {
	s.EnableFast = true
}
