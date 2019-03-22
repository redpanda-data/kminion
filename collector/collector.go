package collector

import (
	"fmt"
	"github.com/google-cloud-tools/kafka-minion/kafka"
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/google-cloud-tools/kafka-minion/storage"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"strconv"
)

var (
	groupPartitionOffsetDesc *prometheus.Desc
	groupPartitionLagDesc    *prometheus.Desc
	partitionWaterMarksDesc  *prometheus.Desc
)

// Collector collects and provides all Kafka metrics on each /metrics invocation, see:
// https://godoc.org/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
type Collector struct {
	opts    *options.Options
	storage *storage.OffsetStorage
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
func NewCollector(opts *options.Options, storage *storage.OffsetStorage) *Collector {
	groupPartitionOffsetDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_partition", "offset"),
		"Newest commited offset of a consumer group for a partition",
		[]string{"group", "group_is_latest", "group_version", "topic", "partition", "last_commit"}, prometheus.Labels{},
	)
	groupPartitionLagDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "group_partition", "lag"),
		"Number of messages the consumer group is behind",
		[]string{"group", "group_is_latest", "group_version", "topic", "partition", "last_commit"}, prometheus.Labels{},
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

// Collect is triggered by the Prometheus registry when the metrics endpoint has been invoked
func (e *Collector) Collect(ch chan<- prometheus.Metric) {
	log.Debug("Collector's collect has been invoked")
	offsets := e.storage.ConsumerOffsets()
	consumerGroups := getVersionedConsumerGroups(offsets)
	partitionHighWaterMarks := e.storage.PartitionHighWaterMarks()

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
			strconv.FormatInt(offset.Timestamp, 10),
		)

		// Key in partition water mark map is "topic:partition"
		key := fmt.Sprintf("%v:%v", offset.Topic, offset.Partition)
		partitionHighWaterMark := partitionHighWaterMarks[key].HighWaterMark
		lag := offset.Offset - partitionHighWaterMark
		// Lag might be below zero because partitionHighWaterMark might by older than last commited offset
		if lag < 0 {
			lag = 0
		}

		ch <- prometheus.MustNewConstMetric(
			groupPartitionLagDesc,
			prometheus.GaugeValue,
			float64(lag),
			offset.Group,
			strconv.FormatBool(group.IsLatest),
			strconv.Itoa(int(group.Version)),
			offset.Topic,
			strconv.Itoa(int(offset.Partition)),
			strconv.FormatInt(offset.Timestamp, 10),
		)
	}

	for _, partition := range partitionHighWaterMarks {
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

func getVersionedConsumerGroups(offsets map[string]kafka.ConsumerPartitionOffset) map[string]*versionedConsumerGroup {
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

	return parseVersion(groupName, newVersionedString, indexCursor)
}
