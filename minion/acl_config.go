package minion

type ACLsConfig struct {
	Enabled bool `koanf:"enabled"`
}

func (c *ACLsConfig) Validate() error {
	return nil
}

func (c *ACLsConfig) SetDefaults() {
	c.Enabled = false
}
