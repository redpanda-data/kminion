package collector

import (
	"github.com/google-cloud-tools/kafka-minion/kafka"
	"github.com/google-cloud-tools/kafka-minion/options"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

var (
	topicPartitionCountDesc         *prometheus.Desc
	topicMessageCountDesc           *prometheus.Desc
	consumerGroupTopicLagDesc       *prometheus.Desc
	latestConsumerGroupTopicLagDesc *prometheus.Desc
)

// Collector collects and provides all Kafka metrics
type Collector struct {
	kafkaClient *kafka.Client
}

// NewKafkaCollector creates a new instance of our internal KafkaCollector
func NewKafkaCollector(opts *options.Options) (*Collector, error) {
	kafkaClient, err := kafka.NewKafkaClient(opts)
	if err != nil {
		return nil, err
	}

	// Initialize all metric types
	topicPartitionCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic", "partition_count"),
		"Number of partitions for this topic",
		[]string{"topic"}, prometheus.Labels{},
	)
	topicMessageCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "topic", "message_count"),
		"Number of expected messages on a given topic (not reliable on compacted topics)",
		[]string{"topic"}, prometheus.Labels{},
	)
	consumerGroupTopicLagDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.MetricsPrefix, "consumer_group", "topic_lag"),
		"Current approximate lag of a consumergroup for a topic",
		[]string{"consumer_group", "consumer_group_base_name", "topic", "consumer_group_version", "is_latest_consumer_group"}, prometheus.Labels{},
	)

	kafkaCollector := &Collector{
		kafkaClient: kafkaClient,
	}

	return kafkaCollector, nil
}

// Describe sends a description of all to be exposed metric types to Prometheus
func (e *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- topicPartitionCountDesc
	ch <- topicMessageCountDesc
	ch <- consumerGroupTopicLagDesc
}

// Collect is called by the Prometheus registry when collecting
// metrics. The implementation sends each collected metric via the
// provided channel and returns once the last metric has been sent.
func (e *Collector) Collect(ch chan<- prometheus.Metric) {
	// 1. Get a fresh copy of all available topic names
	topicNames, err := e.kafkaClient.GetTopicNames()
	if err != nil {
		log.Error(err)
	}

	// 2. Get partition ids for all topics and expose the partition count by topic metric
	partitionIDsByTopicName := e.kafkaClient.GetPartitionIDsBulk(topicNames)
	for topicName, partitionIDs := range partitionIDsByTopicName {
		ch <- prometheus.MustNewConstMetric(
			topicPartitionCountDesc,
			prometheus.GaugeValue,
			float64(len(partitionIDs)),
			topicName,
		)
	}

	// Get partition details for all partitions in all topics
	topicsByName := e.kafkaClient.GetPartitionOffsets(partitionIDsByTopicName)
	for topicName, topic := range topicsByName {
		ch <- prometheus.MustNewConstMetric(
			topicMessageCountDesc,
			prometheus.GaugeValue,
			float64(topic.MessageCount),
			topicName,
		)
	}

	log.Debugf("Collecting consumer group metrics")
	consumerGroupTopicLagsByGroupName := e.kafkaClient.ConsumerGroupTopicLags(topicsByName)
	latestConsumerGroupsByName := getLatestConsumerGroupsByName(consumerGroupTopicLagsByGroupName)

	for _, topicLags := range consumerGroupTopicLagsByGroupName {
		for _, value := range topicLags {
			isLatest := "false"
			baseName := value.Name
			version := uint8(0)

			// If this group is the latest consumer group also add it with a different metrics description
			if consumerGroup, ok := latestConsumerGroupsByName[value.Name]; ok {
				isLatest = "true"
				baseName = consumerGroup.BaseName
				version = consumerGroup.Version
			}

			ch <- prometheus.MustNewConstMetric(
				consumerGroupTopicLagDesc,
				prometheus.GaugeValue,
				float64(value.TopicLag),
				value.Name,
				baseName,
				value.TopicName,
				strconv.Itoa(int(version)),
				isLatest,
			)
		}
	}
}

// versionedConsumerGroup contains information about the consumer group's base name and version
type versionedConsumerGroup struct {
	BaseName string
	Name     string
	Version  uint8
}

// getLatestConsumerGroupNames returns the latest consumer group names in a map where the consumer group name is the key.
func getLatestConsumerGroupsByName(groupsByTopicName map[string][]*kafka.ConsumerGroupTopicLag) map[string]*versionedConsumerGroup {
	latestConsumerGroupByBaseName := make(map[string]*versionedConsumerGroup)

	for _, groups := range groupsByTopicName {
		for _, group := range groups {
			consumerGroup := getVersionedConsumerGroup(group.Name)
			baseName := consumerGroup.BaseName
			// Check if there already is a potentially latest consumer group for this base name
			if _, ok := latestConsumerGroupByBaseName[baseName]; ok {
				// Only overwrite base consumer group if this consumergroup version is higher
				if latestConsumerGroupByBaseName[baseName].Version < consumerGroup.Version {
					latestConsumerGroupByBaseName[baseName] = consumerGroup
				}
			} else {
				latestConsumerGroupByBaseName[baseName] = consumerGroup
			}
		}
	}

	// we already got consumer groups by base name, but now we want them in a map grouped by their actual group name as key
	consumerGroupByName := make(map[string]*versionedConsumerGroup)
	for _, group := range latestConsumerGroupByBaseName {
		consumerGroupByName[group.Name] = group
	}

	return consumerGroupByName
}

// getVersionedConsumerGroup returns the "base name" of a consumer group and it's version
func getVersionedConsumerGroup(consumerGroupName string) *versionedConsumerGroup {
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
	return &versionedConsumerGroup{BaseName: baseName, Name: consumerGroupName, Version: uint8(parsedVersion)}
}

// IsHealthy returns a bool which indicates if the collector is in a healthy state or not
func (e *Collector) IsHealthy() bool {
	return e.kafkaClient.IsHealthy()
}
