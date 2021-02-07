package minion

type LogDirsConfig struct {
	// Enabled specifies whether log dirs shall be scraped and exported or not. This should be disabled for clusters prior
	// to version 1.0.0 as describing log dirs was not supported back then.
	Enabled bool `koanf:"enabled"`
}

// Validate if provided LogDirsConfig is valid.
func (c *LogDirsConfig) Validate() error {
	return nil
}

// SetDefaults for topic config
func (c *LogDirsConfig) SetDefaults() {
	c.Enabled = true
}
