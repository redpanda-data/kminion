package collector

import (
	"strconv"

	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/google-cloud-tools/kafka-minion/storage"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	// Consumer group metrics
	groupPartitionOffsetDesc      *prometheus.Desc
	groupPartitionCommitCountDesc *prometheus.Desc
	groupPartitionLastCommitDesc  *prometheus.Desc
	groupPartitionLagDesc         *prometheus.Desc
	groupTopicLagDesc             *prometheus.Desc

	// Topic metrics
	partitionCountDesc        *prometheus.Desc
	subscribedGroupsCountDesc *prometheus.Desc

	// Partition metrics
	partitionLowWaterMarkDesc  *prometheus.Desc
	partitionHighWaterMarkDesc *prometheus.Desc
	partitionMessageCountDesc  *prometheus.Desc
)

// Collector collects and provides all Kafka metrics on each /metrics invocation, see:
// https://godoc.org/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
type Collector struct {
	opts    *options.Options
	storage *storage.MemoryStorage
	logger  *log.Entry
}

// versionedConsumerGroup represents the information which one could interpret by looking at all consumer group names
// For instance consumer group name "sample-group-1" has base name "sample-group-", version: 1 and is the latest as long
// as there is no group with the same base name and a higher appending number than 1
type versionedConsumerGroup struct {
	BaseName string
	Name     string
	Version  uint32
	IsLatest bool
}

// NewCollector returns a new prometheus collector, preinitialized with all the to be exposed metrics under respect
// of the metrics prefix which can be passed via environment variables
func NewCollector(opts *options.Options, storage *storage.MemoryStorage) *Collector {
	logger := log.WithFields(log.Fields{
		"module": "collector",
	})

	// Consumer group metrics
	groupPartitionOffsetDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "offset"),
		"Newest committed offset of a consumer group for a partition",
		[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
	)
	groupPartitionCommitCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "commit_count"),
		"Number of commits of a consumer group for a partition",
		[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
	)
	groupPartitionLastCommitDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "last_commit"),
		"Timestamp when consumer group last committed an offset for a partition",
		[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
	)
	groupPartitionLagDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_topic_partition", "lag"),
		"Number of messages the consumer group is behind for a partition",
		[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
	)
	groupTopicLagDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_topic", "lag"),
		"Number of messages the consumer group is behind for a topic",
		[]string{"group", "group_base_name", "group_is_latest", "group_version", "topic"}, prometheus.Labels{},
	)

	// Topic metrics
	partitionCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic", "partition_count"),
		"Partition count for a given topic along with cleanup policy as label",
		[]string{"topic", "cleanup_policy"}, prometheus.Labels{},
	)
	subscribedGroupsCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic", "subscribed_groups_count"),
		"Number of consumer groups which have at least one consumer group offset for any of the topics partitions",
		[]string{"topic"}, prometheus.Labels{},
	)

	// Partition metrics
	partitionHighWaterMarkDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "high_water_mark"),
		"Highest known committed offset for this partition",
		[]string{"topic", "partition"}, prometheus.Labels{},
	)
	partitionLowWaterMarkDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "low_water_mark"),
		"Oldest known committed offset for this partition",
		[]string{"topic", "partition"}, prometheus.Labels{},
	)
	partitionMessageCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "message_count"),
		"Number of messages for a given topic. Calculated by subtracting high water mark by low water mark.",
		[]string{"topic", "partition"}, prometheus.Labels{},
	)

	return &Collector{
		opts,
		storage,
		logger,
	}
}

// Describe sends a description of all to be exposed metric types to Prometheus
func (e *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- groupPartitionOffsetDesc
}

// Collect is triggered by the Prometheus registry when the metrics endpoint has been invoked
func (e *Collector) Collect(ch chan<- prometheus.Metric) {
	log.Debug("Collector's collect has been invoked")

	if e.storage.IsConsumed() == false {
		log.Info("Offets topic has not yet been consumed until the end")
		return
	}

	consumerOffsets := e.storage.ConsumerOffsets()
	partitionLowWaterMarks := e.storage.PartitionLowWaterMarks()
	partitionHighWaterMarks := e.storage.PartitionHighWaterMarks()
	topicConfigs := e.storage.TopicConfigs()

	e.collectConsumerOffsets(ch, consumerOffsets, partitionLowWaterMarks, partitionHighWaterMarks)

	for _, config := range topicConfigs {
		ch <- prometheus.MustNewConstMetric(
			partitionCountDesc,
			prometheus.GaugeValue,
			float64(config.PartitionCount),
			config.TopicName,
			config.CleanupPolicy,
		)
	}

	for _, partitions := range partitionLowWaterMarks {
		for _, partition := range partitions {
			ch <- prometheus.MustNewConstMetric(
				partitionLowWaterMarkDesc,
				prometheus.GaugeValue,
				float64(partition.WaterMark),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)
		}
	}

	for _, partitions := range partitionHighWaterMarks {
		for _, partition := range partitions {
			ch <- prometheus.MustNewConstMetric(
				partitionHighWaterMarkDesc,
				prometheus.GaugeValue,
				float64(partition.WaterMark),
				partition.TopicName,
				strconv.Itoa(int(partition.PartitionID)),
			)
		}
	}

	for _, partitions := range partitionHighWaterMarks {
		for _, partition := range partitions {
			topicName := partition.TopicName
			partitionID := partition.PartitionID
			if lowWaterMark, exists := partitionLowWaterMarks[topicName][partitionID]; exists {
				ch <- prometheus.MustNewConstMetric(
					partitionMessageCountDesc,
					prometheus.GaugeValue,
					float64(partition.WaterMark-lowWaterMark.WaterMark),
					partition.TopicName,
					strconv.Itoa(int(partition.PartitionID)),
				)
			}
		}
	}
}

type groupLag struct {
	versionedGroup *versionedConsumerGroup
	lagByTopic     map[string]int64
}

func (e *Collector) collectConsumerOffsets(ch chan<- prometheus.Metric, offsets map[string]storage.ConsumerPartitionOffsetMetric,
	lowWaterMarks map[string]storage.PartitionWaterMarks, highWaterMarks map[string]storage.PartitionWaterMarks) {
	consumerGroups := getVersionedConsumerGroups(offsets)

	errorTopics := make(map[string]bool)
	groupLagsByGroupName := make(map[string]groupLag)

	// Partition offsets and lags
	for _, offset := range offsets {
		group := consumerGroups[offset.Group]
		// Offset metric
		ch <- prometheus.MustNewConstMetric(
			groupPartitionOffsetDesc,
			prometheus.GaugeValue,
			float64(offset.Offset),
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)

		// Commit count metric
		ch <- prometheus.MustNewConstMetric(
			groupPartitionCommitCountDesc,
			prometheus.CounterValue,
			offset.TotalCommitCount,
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)

		// Last commit metric
		ch <- prometheus.MustNewConstMetric(
			groupPartitionLastCommitDesc,
			prometheus.GaugeValue,
			float64(offset.Timestamp),
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)

		if _, exists := lowWaterMarks[offset.Topic][offset.Partition]; !exists {
			errorTopics[offset.Topic] = true
			e.logger.WithFields(log.Fields{
				"topic":     offset.Topic,
				"partition": offset.Partition,
			}).Warn("could not calculate partition lag because low water mark is missing")
			continue
		}
		partitionLowWaterMark := lowWaterMarks[offset.Topic][offset.Partition].WaterMark
		if _, exists := highWaterMarks[offset.Topic][offset.Partition]; !exists {
			errorTopics[offset.Topic] = true
			e.logger.WithFields(log.Fields{
				"topic":     offset.Topic,
				"partition": offset.Partition,
			}).Warn("could not calculate partition lag because high water mark is missing")
			continue
		}
		partitionHighWaterMark := highWaterMarks[offset.Topic][offset.Partition].WaterMark

		var lag int64
		if offset.Offset > partitionHighWaterMark {
			// Partition offsets are updated periodically, while consumer offsets continuously flow in. Hence it's possible
			// that consumer offset might be ahead of the partition high watermark. For this case mark it as zero lag
			lag = 0
		} else if offset.Offset < partitionLowWaterMark {
			// If last committed offset does not exist anymore due to delete policy (e. g. 1day retention, 3day old commit)
			lag = partitionHighWaterMark - partitionLowWaterMark
		} else {
			lag = partitionHighWaterMark - offset.Offset
		}

		// Add partition lag to group:topic lag aggregation
		if _, exists := groupLagsByGroupName[offset.Group]; !exists {
			groupLagsByGroupName[offset.Group] = groupLag{
				versionedGroup: group,
				lagByTopic:     make(map[string]int64),
			}
		}
		groupLagsByGroupName[offset.Group].lagByTopic[offset.Topic] += lag

		ch <- prometheus.MustNewConstMetric(
			groupPartitionLagDesc,
			prometheus.GaugeValue,
			float64(lag),
			offset.Group,
			group.BaseName,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)
	}

	// Group lags and subscribed consumer group count per topicname
	subscribedGroupsByTopic := make(map[string]int32)
	for groupName, groupLag := range groupLagsByGroupName {
		for topicName, topicLag := range groupLag.lagByTopic {
			// Bump subscribed consumer group count for this topic
			subscribedGroupsByTopic[topicName]++

			if _, hasErrors := errorTopics[topicName]; hasErrors {
				e.logger.WithFields(log.Fields{
					"group": groupName,
					"topic": topicName,
				}).Warn("cannot calculate group lag due to a missing partition watermark")
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				groupTopicLagDesc,
				prometheus.GaugeValue,
				float64(topicLag),
				groupLag.versionedGroup.Name,
				groupLag.versionedGroup.BaseName,
				strconv.FormatBool(groupLag.versionedGroup.IsLatest),
				strconv.Itoa(int(groupLag.versionedGroup.Version)),
				topicName,
			)
		}
	}

	for topicName := range lowWaterMarks {
		subscribedGroups := subscribedGroupsByTopic[topicName]

		ch <- prometheus.MustNewConstMetric(
			subscribedGroupsCountDesc,
			prometheus.GaugeValue,
			float64(subscribedGroups),
			topicName,
		)
	}
}

func getVersionedConsumerGroups(offsets map[string]storage.ConsumerPartitionOffsetMetric) map[string]*versionedConsumerGroup {
	// This map contains all known consumer groups. Key is the full group name
	groupsByName := make(map[string]*versionedConsumerGroup)

	// This map is supposed to contain only the highest versioned consumer within a consumer group base name
	latestGroupByBaseName := make(map[string]*versionedConsumerGroup)
	for _, offset := range offsets {
		consumerGroup := parseConsumerGroupName(offset.Group)
		groupsByName[offset.Group] = consumerGroup
		baseName := consumerGroup.BaseName
		if _, ok := latestGroupByBaseName[baseName]; ok {
			// Overwrite entry for this base name if consumergroup version is higher
			if latestGroupByBaseName[baseName].Version < consumerGroup.Version {
				latestGroupByBaseName[baseName] = consumerGroup
			}
		} else {
			latestGroupByBaseName[baseName] = consumerGroup
		}
	}

	// Set IsLatest if this consumer group is the highest known version within this group base name
	for _, group := range latestGroupByBaseName {
		groupsByName[group.Name].IsLatest = true
	}

	return groupsByName
}

// parseConsumerGroupName returns the "base name" of a consumer group and it's version
// Given the name "sample-group-01" the base name would be "sample-group" and the version is "1"
// If there's no appending number it's being considered as version 0
func parseConsumerGroupName(consumerGroupName string) *versionedConsumerGroup {
	parsedVersion, baseName := parseVersion(consumerGroupName, "", len(consumerGroupName)-1)
	return &versionedConsumerGroup{BaseName: baseName, Name: consumerGroupName, Version: uint32(parsedVersion), IsLatest: false}
}

// parseVersion tries to parse a "version" from a consumer group name. An appending number of a
// consumer group name is considered as it's version. It returns the parsed version and the consumer group base name.
func parseVersion(groupName string, versionString string, digitIndexCursor int) (uint32, string) {
	if len(groupName) == 0 {
		return 0, ""
	}

	// Try to parse a digit from right to left, so that we correctly identify names like "consumer-group-v003" as well
	lastCharacter := groupName[digitIndexCursor : digitIndexCursor+1]
	_, err := strconv.Atoi(lastCharacter)
	if err != nil {
		if len(versionString) == 0 {
			return 0, groupName[0 : digitIndexCursor+1]
		}

		// We've got a versionString, but this character is no digit anymore
		version, err := strconv.ParseUint(versionString, 10, 0)
		if err != nil {
			// should never happen, because version string must only consist of valid ints
			return 0, groupName[0:digitIndexCursor]
		}
		return uint32(version), groupName[0 : digitIndexCursor+1]
	}

	// Last character is a valid digit, so we can prepend it to the "versionString" which we can try to
	// parse as int when we are done
	newVersionedString := lastCharacter + versionString
	indexCursor := digitIndexCursor - 1

	// If we've got a consumer group name which only has digits, the indexCursor will ultimately be -1
	if indexCursor < 0 {
		return 0, groupName
	}

	return parseVersion(groupName, newVersionedString, indexCursor)
}
