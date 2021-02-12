package prometheus

import (
	"context"
	"github.com/cloudhut/kminion/v2/minion"
	uuid2 "github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"os"
	"time"
)

// Exporter is the Prometheus exporter that implements the prometheus.Collector interface
type Exporter struct {
	cfg       Config
	logger    *zap.Logger
	minionSvc *minion.Service

	// Exporter metrics
	exporterUp            *prometheus.Desc
	failedCollectsCounter *prometheus.CounterVec

	// Kafka metrics
	clusterInfo *prometheus.Desc
	brokerInfo  *prometheus.Desc

	brokerLogDirSize *prometheus.Desc
	topicLogDirSize  *prometheus.Desc

	topicHighWaterMarkSum  *prometheus.Desc
	partitionHighWaterMark *prometheus.Desc
	topicLowWaterMarkSum   *prometheus.Desc
	partitionLowWaterMark  *prometheus.Desc

	consumerGroupInfo              *prometheus.Desc
	consumerGroupTopicPartitionLag *prometheus.Desc
	consumerGroupTopicLag          *prometheus.Desc
}

func NewExporter(cfg Config, logger *zap.Logger, minionSvc *minion.Service) (*Exporter, error) {
	return &Exporter{cfg: cfg, logger: logger, minionSvc: minionSvc}, nil
}

func (e *Exporter) InitializeMetrics() {
	e.exporterUp = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "exporter", "up"),
		"Build info about this Prometheus Exporter. Gauge value is 0 if one or more scrapes have failed.",
		nil,
		map[string]string{"version": os.Getenv("EXPORTER_VERSION")},
	)
	e.failedCollectsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: e.cfg.Namespace,
			Subsystem: "kafka",
			Name:      "failed_collects_total",
			Help:      "Number of collects that have failed",
		},
		[]string{"type"},
	)
	prometheus.MustRegister(e.failedCollectsCounter)

	// Kafka metrics
	e.clusterInfo = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "cluster_info"),
		"Kafka cluster information",
		[]string{"cluster_version", "broker_count", "controller_id", "cluster_id"},
		nil,
	)
	e.brokerInfo = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "broker_info"),
		"Kafka broker information",
		[]string{"broker_id", "address", "port", "rack_id", "is_controller"},
		nil,
	)
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
	e.partitionLowWaterMark = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_partition_low_water_mark"),
		"Partition Low Water Mark",
		[]string{"topic_name", "partition_id"},
		nil,
	)
	e.topicLowWaterMarkSum = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_low_water_mark_sum"),
		"Sum of all the topic's partition low water marks",
		[]string{"topic_name"},
		nil,
	)
	e.partitionHighWaterMark = prometheus.NewDesc(
		prometheus.BuildFQName(e.cfg.Namespace, "kafka", "topic_partition_high_water_mark"),
		"Partition High Water Mark",
		[]string{"topic_name", "partition_id"},
		nil,
	)
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
		[]string{"group_id", "member_count", "protocol", "protocol_type", "state"},
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

	uuid := uuid2.New()
	ctx = context.WithValue(ctx, "requestId", uuid.String())

	ok := e.collectClusterInfo(ctx, ch)
	ok = e.collectBrokerInfo(ctx, ch) && ok
	ok = e.collectLogDirs(ctx, ch) && ok
	ok = e.collectConsumerGroups(ctx, ch) && ok
	ok = e.collectTopicPartitionOffsets(ctx, ch) && ok
	ok = e.collectConsumerGroupLags(ctx, ch) && ok

	if ok {
		ch <- prometheus.MustNewConstMetric(e.exporterUp, prometheus.GaugeValue, 1.0)
	} else {
		ch <- prometheus.MustNewConstMetric(e.exporterUp, prometheus.GaugeValue, 0.0)
	}
}
