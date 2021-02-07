package prometheus

type Config struct {
	Namespace string `koanf:"namespace"`
}

func (c *Config) SetDefaults() {
	c.Namespace = "kminion"
}
