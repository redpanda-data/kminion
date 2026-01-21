package minion

import (
	"fmt"
)

type LogDirsConfig struct {
	// Enabled specifies whether log dirs shall be scraped and exported or not. This should be disabled for clusters prior
	// to version 1.0.0 as describing log dirs was not supported back then.
	Enabled bool `koanf:"enabled"`
	// AllowedTopics are regex strings of topic names whose topic metrics that shall be exported.
	AllowedTopics []string `koanf:"allowedTopics"`

	// IgnoredTopics are regex strings of topic names that shall be ignored/skipped when exporting metrics. Ignored topics
	// take precedence over allowed topics.
	IgnoredTopics []string `koanf:"ignoredTopics"`
}

// Validate if provided LogDirsConfig is valid.
func (c *LogDirsConfig) Validate() error {
	// Check whether each provided string is valid regex
	for _, topic := range c.AllowedTopics {
		_, err := compileRegex(topic)
		if err != nil {
			return fmt.Errorf("allowed topic string '%v' is not valid regex", topic)
		}
	}

	for _, topic := range c.IgnoredTopics {
		_, err := compileRegex(topic)
		if err != nil {
			return fmt.Errorf("ignored topic string '%v' is not valid regex", topic)
		}
	}
	return nil
}

// SetDefaults for topic config
func (c *LogDirsConfig) SetDefaults() {
	c.Enabled = true
	c.AllowedTopics = []string{"/.*/"}
}
