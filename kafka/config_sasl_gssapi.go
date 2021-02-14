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
}
