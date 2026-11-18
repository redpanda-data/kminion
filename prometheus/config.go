package prometheus

type Config struct {
	Host      string `koanf:"host"`
	Port      int    `koanf:"port"`
	Namespace string `koanf:"namespace"`
	// WebConfigFilepath is the path of a Prometheus web config file with the TLS settings of the HTTP server.
	// Empty means plain HTTP.
	WebConfigFilepath string `koanf:"webConfigFilepath"`
}

func (c *Config) SetDefaults() {
	c.Port = 8080
	c.Namespace = "kminion"
}
