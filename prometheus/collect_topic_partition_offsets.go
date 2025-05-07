package prometheus

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/cloudhut/kminion/v2/minion"
)

func (e *Exporter) collectTopicPartitionOffsets(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.Topics.Enabled {
		return true
	}

	isOk := true

	// Low Watermarks
	lowWaterMarks, err := e.minionSvc.ListStartOffsetsCached(ctx)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}
	// High Watermarks
	highWaterMarks, err := e.minionSvc.ListEndOffsetsCached(ctx)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}

	// Process Low Watermarks

	for topicName, partitions := range lowWaterMarks {
		if !e.minionSvc.IsTopicAllowed(topicName) {
			continue
		}

		waterMarkSum := int64(0)
		hasErrors := false
		for _, offset := range partitions {
			if offset.Err != nil {
				hasErrors = true
				isOk = false
				continue
			}
			waterMarkSum += offset.Offset
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				e.partitionLowWaterMark,
				prometheus.GaugeValue,
				float64(offset.Offset),
				topicName,
				strconv.Itoa(int(offset.Partition)),
			)
		}
		// We only want to report the sum of all partition marks if we receive watermarks from all partition
		if !hasErrors {
			ch <- prometheus.MustNewConstMetric(
				e.topicLowWaterMarkSum,
				prometheus.GaugeValue,
				float64(waterMarkSum),
				topicName,
			)
		}
	}

	for topicName, partitions := range highWaterMarks {
		if !e.minionSvc.IsTopicAllowed(topicName) {
			continue
		}
		waterMarkSum := int64(0)
		hasErrors := false
		for _, offset := range partitions {
			if offset.Err != nil {
				hasErrors = true
				isOk = false
				continue
			}
			waterMarkSum += offset.Offset
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				e.partitionHighWaterMark,
				prometheus.GaugeValue,
				float64(offset.Offset),
				topicName,
				strconv.Itoa(int(offset.Partition)),
			)
		}
		// We only want to report the sum of all partition marks if we receive watermarks from all partitions
		if !hasErrors {
			ch <- prometheus.MustNewConstMetric(
				e.topicHighWaterMarkSum,
				prometheus.GaugeValue,
				float64(waterMarkSum),
				topicName,
			)
		}
	}

	return isOk
}
