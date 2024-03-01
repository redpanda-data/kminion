package minion

type BrokerInfoConfig struct {
	// Enabled specifies whether broker information shall be scraped and exported or not.
	Enabled bool `koanf:"enabled"`
}

func (c *BrokerInfoConfig) SetDefaults() {
	c.Enabled = true
}

func (c *BrokerInfoConfig) Validate() error {
	return nil
}
