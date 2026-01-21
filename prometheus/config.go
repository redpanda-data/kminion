package prometheus

type Config struct {
	Host        string `koanf:"host"`
	Port        int    `koanf:"port"`
	Namespace   string `koanf:"namespace"`
	TLSCertFile string `koanf:"tlsCertificate"`
	TLSKeyFile  string `koanf:"tlsKey"`
}

func (c *Config) SetDefaults() {
	c.Port = 8080
	c.Namespace = "kminion"
}
