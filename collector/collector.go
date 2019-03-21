package collector

import (
	"github.com/google-cloud-tools/kafka-minion/kafka"
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/google-cloud-tools/kafka-minion/storage"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

var (
	groupPartitionOffsetDesc *prometheus.Desc
	partitionWaterMarksDesc  *prometheus.Desc
)

// Collector collects and provides all Kafka metrics on each /metrics invocation
type Collector struct {
	opts    *options.Options
	storage *storage.OffsetStorage
}

// versionedConsumerGroup contains information about the consumer group's base name and version
type versionedConsumerGroup struct {
	BaseName string
	Name     string
	Version  uint32
	IsLatest bool
}

// NewCollector returns a new prometheus collector, preinitialized with all the to be exposed metrics
func NewCollector(opts *options.Options, storage *storage.OffsetStorage) *Collector {
	groupPartitionOffsetDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group", "partition_offset"),
		"Newest commited offset of a consumer group for a partition",
		[]string{"group", "group_is_latest", "group_version", "topic", "partition"}, prometheus.Labels{},
	)
	partitionWaterMarksDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic_partition", "high_water_mark"),
		"Highest known commited offset for this partition",
		[]string{"topic", "partition", "timestamp"}, prometheus.Labels{},
	)

	return &Collector{opts, storage}
}

// Describe sends a description of all to be exposed metric types to Prometheus
func (e *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- groupPartitionOffsetDesc
}

// Collect is called by the Prometheus registry when collecting
// metricses. The implementation sends each collected metric via the
// provided channel and returns once the last metric has been sent.
func (e *Collector) Collect(ch chan<- prometheus.Metric) {
	log.Debug("Collector's collect has been invoked")
	offsets := e.storage.ConsumerOffsets()
	consumerGroups := getVersionedConsumerGroups(offsets)
	for _, offset := range offsets {
		group := consumerGroups[offset.Group]
		ch <- prometheus.MustNewConstMetric(
			groupPartitionOffsetDesc,
			prometheus.GaugeValue,
			float64(offset.Offset),
			offset.Group,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
		)
	}

	partitionWaterMarks := e.storage.PartitionWaterMarks()
	for _, partition := range partitionWaterMarks {
		ch <- prometheus.MustNewConstMetric(
			partitionWaterMarksDesc,
			prometheus.GaugeValue,
			float64(partition.HighWaterMark),
			partition.TopicName,
			strconv.Itoa(int(partition.PartitionID)),
			strconv.Itoa(int(partition.Timestamp)),
		)
	}
}

func getVersionedConsumerGroups(offsets map[string]*kafka.ConsumerPartitionOffset) map[string]*versionedConsumerGroup {
	// This map contains all known consumer groups. Key is the actual group name
	groupsByName := make(map[string]*versionedConsumerGroup)

	// We need the below paragrahp to determine the highest versioned consumer groups for each group base name
	// Therefore we must completely iterate over all known consumer group names at least once

	// This map is supposed to contain only the highest versioned consumer group for each group base name
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

	// We got two maps of versioned consumer groups now. One map contains all consumer groups,
	// the other just those who have the highest version. We want to mark the consumer group as
	// IsLatest if this consumer group is the highest known version
	for _, group := range latestGroupByBaseName {
		groupsByName[group.Name].IsLatest = true
	}

	return groupsByName
}

// parseConsumerGroupName returns the "base name" of a consumer group and it's version
// Given the name "sample-group-01" the base name would be "sample-group" and the version is "1"
// If there's no appending number it's being considered as version 0
func parseConsumerGroupName(consumerGroupName string) *versionedConsumerGroup {
	baseName := consumerGroupName
	parsedVersion := 0

	lastDashIndex := strings.LastIndex(consumerGroupName, "-")
	if lastDashIndex > 0 {
		// Potentially this is our base name (if the group has no trailing number at all, this is wrong though)
		baseName = consumerGroupName[:lastDashIndex]
		potentialVersion := consumerGroupName[lastDashIndex+1 : len(consumerGroupName)]
		var err error
		parsedVersion, err = strconv.Atoi(potentialVersion)
		if err != nil {
			parsedVersion = 0
			baseName = consumerGroupName
		}
	}
	return &versionedConsumerGroup{BaseName: baseName, Name: consumerGroupName, Version: uint32(parsedVersion), IsLatest: false}
}

// parseVersion tries to parse a "version" from a consumer group name. An appending number of a consumer group name is considered as it's version
func parseVersion(subString string, versionString string) uint32 {
	if len(subString) == 0 {
		return 0
	}

	// Try to parse a digit from right to left, so that we correctly identify names like "consumer-group-v003" as well
	lastCharacter := subString[len(subString)-1:]
	digit, err := strconv.Atoi(lastCharacter)
	if err != nil {
		if len(versionString) == 0 {
			return 0
		}

		version, err := strconv.ParseUint(versionString, 10, 0)
		if err != nil {
			// should never happen, because version string must only consist of valid ints
			return 0
		}
		return uint32(version)
	}

	// It's a valid digit, so we can prepend it to the "versionString" which we can try to parse as int when we are done
	newVersionedString := lastCharacter + versionString
	remainingSubString := subString[0 : len(subString)-1]

	return parseVersion(remainingSubString, newVersionedString)
}
