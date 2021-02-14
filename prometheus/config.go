package prometheus

type Config struct {
	Host      string `koanf:"host"`
	Port      int    `koanf:"port"`
	Namespace string `koanf:"namespace"`
}

func (c *Config) SetDefaults() {
	c.Port = 8080
	c.Namespace = "kminion"
}
