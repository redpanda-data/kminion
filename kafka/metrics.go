package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

// This file creates prometheus metrics about the internal state of kafka minion:
// - How many kafka messages have been consumed (successfully and failed)
// - How many offset commits (tombstones) have been decoded
// - How many group metadata (tombstones) have been decoded

const internalMetricsName = "kafka_minion_internal"

var (
	offsetCommit = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(internalMetricsName, "offset_consumer", "offset_commits_read"),
		Help: "Number of read offset commits",
	}, []string{"version"})
	offsetCommitTombstone = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(internalMetricsName, "offset_consumer", "offset_commits_tombstones_read"),
		Help: "Number of read group offset commit tombstone messages",
	})

	groupMetadata = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(internalMetricsName, "offset_consumer", "group_metadata_read"),
		Help: "Number of read group meta data messages",
	}, []string{"version"})
	groupMetadataTombstone = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(internalMetricsName, "offset_consumer", "group_metadata_tombstones_read"),
		Help: "Number of read group meta data tombstone messages",
	})

	messagesInSuccess = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(internalMetricsName, "kafka", "messages_in_success"),
		Help: "Number of messages successfully consumed from a topic",
	}, []string{"topic"})
	messagesInFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName(internalMetricsName, "kafka", "messages_in_failed"),
		Help: "Number of messages failed to consume from a topic",
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(offsetCommit)
	prometheus.MustRegister(offsetCommitTombstone)

	prometheus.MustRegister(groupMetadata)
	prometheus.MustRegister(groupMetadataTombstone)

	prometheus.MustRegister(messagesInSuccess)
	prometheus.MustRegister(messagesInFailed)
}
