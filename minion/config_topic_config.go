package minion

import "fmt"

const (
	TopicGranularityTopic     string = "topic"
	TopicGranularityPartition string = "partition"
)

type TopicConfig struct {
	// Granularity can be per topic or per partition. If you want to reduce the number of exported metric series and
	// you aren't interested in per partition metrics you could choose "topic".
	Granularity string `koanf:"granularity"`

	// AllowedTopics are regex strings of topic names whose topic metrics that shall be exported.
	AllowedTopics []string `koanf:"allowedTopics"`

	// IgnoredTopics are regex strings of topic names that shall be ignored/skipped when exporting metrics. Ignored topics
	// take precedence over allowed topics.
	IgnoredTopics []string `koanf:"ignoredTopics"`
}

// Validate if provided TopicConfig is valid.
func (c *TopicConfig) Validate() error {
	switch c.Granularity {
	case TopicGranularityPartition, TopicGranularityTopic:
	default:
		return fmt.Errorf("given granularity '%v' is invalid", c.Granularity)
	}

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
func (c *TopicConfig) SetDefaults() {
	c.Granularity = TopicGranularityPartition
	c.AllowedTopics = []string{"/.*/"}
}
