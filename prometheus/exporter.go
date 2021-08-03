package prometheus

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/cloudhut/kminion/v2/minion"
	uuid2 "github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Exporter is the Prometheus exporter that implements the prometheus.Collector interface
type Exporter struct {
	cfg       Config
	logger    *zap.Logger
	minionSvc *minion.Service

	// Exporter metrics
	exporterUp                    *prometheus.Desc
	offsetConsumerRecordsConsumed *prometheus.Desc

	// Kafka metrics
	// General
	clusterInfo *prometheus.Desc
	brokerInfo  *prometheus.Desc

	// Log Dir Sizes
	brokerLogDirSize *prometheus.Desc
	topicLogDirSize  *prometheus.Desc

	// Topic / Partition
	topicInfo              *prometheus.Desc
	topicHighWaterMarkSum  *prometheus.Desc
	partitionHighWaterMark *prometheus.Desc
	topicLowWaterMarkSum   *prometheus.Desc
	partitionLowWaterMark  *prometheus.Desc

	// Consumer Groups
	consumerGroupInfo                    *prometheus.Desc
	consumerGroupMembers                 *prometheus.Desc
	consumerGroupMembersEmpty            *prometheus.Desc
	consumerGroupTopicMembers            *prometheus.Desc
	consumerGroupAssignedTopicPartitions *prometheus.Desc
	consumerGroupTopicOffsetSum          *prometheus.Desc
	consumerGroupTopicPartitionLag       *prometheus.Desc
	consumerGroupTopicLag                *prometheus.Desc
	offsetCommits                        *prometheus.Desc
}

func NewExporter(cfg Config, logger *zap.Logger, minionSvc *minion.Service) (*Exporter, error) {
	return &Exporter{cfg: cfg, logger: logger.Named("prometheus"), minionSvc: minionSvc}, nil
}

func (e *Exporter) InitializeMetrics() {
	// Exporter / internal metrics
	// Exporter up
	e.exporterUp = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "exporter", "up"),
		"Build info about this Prometheus Exporter. Gauge value is 0 if one or more scrapes have failed.",
		nil,
		map[string]string{"version": os.Getenv("VERSION")},
	)
	// OffsetConsumer records consumed
	e.offsetConsumerRecordsConsumed = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "exporter", "offset_consumer_records_consumed_total"),
		"The number of offset records that have been consumed by the internal offset consumer",
		[]string{},
		nil,
	)

	// Kafka metrics
	// Cluster info
	e.clusterInfo = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "cluster_info"),
		"Kafka cluster information",
		[]string{"cluster_version", "broker_count", "controller_id", "cluster_id"},
		nil,
	)
	// Broker Info
	e.brokerInfo = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "broker_info"),
		"Kafka broker information",
		[]string{"broker_id", "address", "port", "rack_id", "is_controller"},
		nil,
	)

	// LogDir sizes
	e.brokerLogDirSize = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "broker_log_dir_size_total_bytes"),
		"The summed size in bytes of all log dirs for a given broker",
		[]string{"broker_id", "address", "port", "rack_id"},
		nil,
	)
	e.topicLogDirSize = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_log_dir_size_total_bytes"),
		"The summed size in bytes of partitions for a given topic. This includes the used space for replica partitions.",
		[]string{"topic_name"},
		nil,
	)

	// Topic / Partition metrics
	// Topic info
	var labels = []string{"topic_name", "partition_count", "replication_factor"}
	for _, key := range e.minionSvc.Cfg.Topics.InfoMetric.ConfigKeys {
		// prometheus does not allow . in label keys
		labels = append(labels, strings.ReplaceAll(key, ".", "_"))
	}
	e.topicInfo = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_info"),
		"Info labels for a given topic",
		labels,
		nil,
	)
	// Partition Low Water Mark
	e.partitionLowWaterMark = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_partition_low_water_mark"),
		"Partition Low Water Mark",
		[]string{"topic_name", "partition_id"},
		nil,
	)
	// Topic Low Water Mark Sum
	e.topicLowWaterMarkSum = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_low_water_mark_sum"),
		"Sum of all the topic's partition low water marks",
		[]string{"topic_name"},
		nil,
	)
	// Partition High Water Mark
	e.partitionHighWaterMark = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_partition_high_water_mark"),
		"Partition High Water Mark",
		[]string{"topic_name", "partition_id"},
		nil,
	)
	// Topic Low Water Mark Sum
	e.topicHighWaterMarkSum = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_high_water_mark_sum"),
		"Sum of all the topic's partition high water marks",
		[]string{"topic_name"},
		nil,
	)

	// Consumer Group Metrics
	// Group Info
	e.consumerGroupInfo = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_info"),
		"Consumer Group info metrics. It will report 1 if the group is in the stable state, otherwise 0.",
		[]string{"group_id", "protocol", "protocol_type", "state", "coordinator_id"},
		nil,
	)
	// Group Members
	e.consumerGroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_members"),
		"Consumer Group member count metrics. It will report the number of members in the consumer group",
		[]string{"group_id"},
		nil,
	)
	// Group Empty Memmbers
	e.consumerGroupMembersEmpty = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_empty_members"),
		"It will report the number of members in the consumer group with no partition assigned",
		[]string{"group_id"},
		nil,
	)
	// Group Topic Members
	e.consumerGroupTopicMembers = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_topic_members"),
		"It will report the number of members in the consumer group assigned on a given topic",
		[]string{"group_id", "topic_name"},
		nil,
	)
	// Group Topic Assigned Partitions
	e.consumerGroupAssignedTopicPartitions = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_topic_assigned_partitions"),
		"It will report the number of partitions assigned in the consumer group for a given topic",
		[]string{"group_id", "topic_name"},
		nil,
	)
	// Topic / Partition Offset Sum (useful for calculating the consumed messages / sec on a topic)
	e.consumerGroupTopicOffsetSum = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_topic_offset_sum"),
		"The sum of all committed group offsets across all partitions in a topic",
		[]string{"group_id", "topic_name"},
		nil,
	)
	// Partition Lag
	e.consumerGroupTopicPartitionLag = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_topic_partition_lag"),
		"The number of messages a consumer group is lagging behind the latest offset of a partition",
		[]string{"group_id", "topic_name", "partition_id"},
		nil,
	)
	// Topic Lag (sum of all partition lags)
	e.consumerGroupTopicLag = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_topic_lag"),
		"The number of messages a consumer group is lagging behind across all partitions in a topic",
		[]string{"group_id", "topic_name"},
		nil,
	)
	// Offset commits by group id
	e.offsetCommits = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "consumer_group_offset_commits_total"),
		"The number of offsets committed by a group",
		[]string{"group_id"},
		nil,
	)

}

// Describe implements the prometheus.Collector interface. It sends the
// super-set of all possible descriptors of metrics collected by this
// Collector to the provided channel and returns once the last descriptor
// has been sent. The sent descriptors fulfill the consistency and uniqueness
// requirements described in the Desc documentation.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.exporterUp
	ch <- e.clusterInfo
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	// Attach a unique id which will be used for caching (and and it's invalidation) of the kafka requests
	uuid := uuid2.New()
	ctx = context.WithValue(ctx, "requestId", uuid.String())

	ok := e.collectClusterInfo(ctx, ch)
	ok = e.collectExporterMetrics(ctx, ch) && ok
	ok = e.collectBrokerInfo(ctx, ch) && ok
	ok = e.collectLogDirs(ctx, ch) && ok
	ok = e.collectConsumerGroups(ctx, ch) && ok
	ok = e.collectTopicPartitionOffsets(ctx, ch) && ok
	ok = e.collectConsumerGroupLags(ctx, ch) && ok
	ok = e.collectTopicInfo(ctx, ch) && ok

	if ok {
		ch <- prometheus.MustNewConstMetric(e.exporterUp, prometheus.GaugeValue, 1.0)
	} else {
		ch <- prometheus.MustNewConstMetric(e.exporterUp, prometheus.GaugeValue, 0.0)
	}
}
