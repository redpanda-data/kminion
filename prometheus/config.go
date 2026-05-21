package prometheus

type Config struct {
	Host            string `koanf:"host"`
	Port            int    `koanf:"port"`
	Namespace       string `koanf:"namespace"`
	TLSEnabled      bool   `koanf:"tlsEnabled"`
	TLSCertFilepath string `koanf:"tlsCertFilepath"`
	TLSKeyFilepath  string `koanf:"tlsKeyFilepath"`
}

func (c *Config) SetDefaults() {
	c.Port = 8080
	c.Namespace = "kminion"
}
