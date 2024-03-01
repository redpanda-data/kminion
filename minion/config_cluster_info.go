package minion

type ClusterInfoConfig struct {
	// Enabled specifies whether cluster information shall be scraped and exported or not.
	Enabled bool `koanf:"enabled"`
}

func (c *ClusterInfoConfig) SetDefaults() {
	c.Enabled = true
}

func (c *ClusterInfoConfig) Validate() error {
	return nil
}
