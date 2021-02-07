package minion

import (
	"fmt"
)

const (
	ConsumerGroupScrapeModeOffsetsTopic string = "offsetsTopics"
	ConsumerGroupScrapeModeAdminAPI     string = "adminApi"

	ConsumerGroupGranularityTopic     string = "topic"
	ConsumerGroupGranularityPartition string = "partition"
)

type ConsumerGroupConfig struct {
	// Enabled specifies whether consumer groups shall be scraped and exported or not.
	Enabled bool `koanf:"enabled"`

	// Mode specifies whether we export consumer group offsets using the Admin API or by consuming the internal
	// __consumer_offsets topic.
	ScrapeMode string `koanf:"scrapeMode"`

	// Granularity can be per topic or per partition. If you want to reduce the number of exported metric series and
	// you aren't interested in per partition lags you could choose "topic" where all partition lags will be summed
	// and only topic lags will be exported.
	Granularity string `koanf:"granularity"`

	// AllowedGroups are regex strings of group ids that shall be exported
	AllowedGroupIDs []string `koanf:"allowedGroups"`

	// IgnoredGroups are regex strings of group ids that shall be ignored/skipped when exporting metrics. Ignored groups
	// take precedence over allowed groups.
	IgnoredGroupIDs []string `koanf:"ignoredGroups"`
}

func (c *ConsumerGroupConfig) SetDefaults() {
	c.Enabled = true
	c.ScrapeMode = ConsumerGroupScrapeModeOffsetsTopic
	c.Granularity = ConsumerGroupGranularityPartition
	c.AllowedGroupIDs = []string{"/.*/"}
}

func (c *ConsumerGroupConfig) Validate() error {
	switch c.ScrapeMode {
	case ConsumerGroupScrapeModeOffsetsTopic, ConsumerGroupScrapeModeAdminAPI:
	default:
		return fmt.Errorf("invalid scrape mode '%v' specified. Valid modes are '%v' or '%v'",
			c.ScrapeMode,
			ConsumerGroupScrapeModeOffsetsTopic,
			ConsumerGroupScrapeModeAdminAPI)
	}

	switch c.Granularity {
	case ConsumerGroupGranularityTopic, ConsumerGroupGranularityPartition:
	default:
		return fmt.Errorf("invalid consumer group granularity '%v' specified. Valid modes are '%v' or '%v'",
			c.Granularity,
			ConsumerGroupGranularityTopic,
			ConsumerGroupGranularityPartition)
	}

	// Check if all group strings are valid regex or literals
	for _, groupID := range c.AllowedGroupIDs {
		_, err := compileRegex(groupID)
		if err != nil {
			return fmt.Errorf("allowed group string '%v' is not valid regex", groupID)
		}
	}

	for _, groupID := range c.IgnoredGroupIDs {
		_, err := compileRegex(groupID)
		if err != nil {
			return fmt.Errorf("ignored group string '%v' is not valid regex", groupID)
		}
	}

	return nil
}
