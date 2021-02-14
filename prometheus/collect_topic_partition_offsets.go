package prometheus

import (
	"context"
	"github.com/cloudhut/kminion/v2/minion"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectTopicPartitionOffsets(ctx context.Context, ch chan<- prometheus.Metric) bool {
	isOk := true

	// Low Watermarks
	lowWaterMarks, err := e.minionSvc.ListOffsetsCached(ctx, -1)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}
	// High Watermarks
	highWaterMarks, err := e.minionSvc.ListOffsetsCached(ctx, -2)
	if err != nil {
		e.logger.Error("failed to fetch low water marks", zap.Error(err))
		return false
	}

	for _, topic := range lowWaterMarks.Topics {
		if !e.minionSvc.IsTopicAllowed(topic.Topic) {
			continue
		}
		waterMarkSum := int64(0)
		for _, partition := range topic.Partitions {
			err := kerr.ErrorForCode(partition.ErrorCode)
			if err != nil {
				e.logger.Error("failed to fetch partition low water mark", zap.Error(err))
				isOk = false
				continue
			}
			waterMarkSum += partition.Offset
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				e.partitionLowWaterMark,
				prometheus.GaugeValue,
				float64(partition.Offset),
				topic.Topic,
				strconv.Itoa(int(partition.Partition)),
			)
		}
		ch <- prometheus.MustNewConstMetric(
			e.topicLowWaterMarkSum,
			prometheus.GaugeValue,
			float64(waterMarkSum),
			topic.Topic,
		)
	}

	for _, topic := range highWaterMarks.Topics {
		if !e.minionSvc.IsTopicAllowed(topic.Topic) {
			continue
		}
		waterMarkSum := int64(0)
		for _, partition := range topic.Partitions {
			err := kerr.ErrorForCode(partition.ErrorCode)
			if err != nil {
				e.logger.Error("failed to fetch partition high water mark", zap.Error(err))
				isOk = true
				continue
			}
			waterMarkSum += partition.Offset
			// Let's end here if partition metrics shall not be exposed
			if e.minionSvc.Cfg.Topics.Granularity == minion.TopicGranularityTopic {
				continue
			}
			ch <- prometheus.MustNewConstMetric(
				e.partitionHighWaterMark,
				prometheus.GaugeValue,
				float64(partition.Offset),
				topic.Topic,
				strconv.Itoa(int(partition.Partition)),
			)
		}
		ch <- prometheus.MustNewConstMetric(
			e.topicHighWaterMarkSum,
			prometheus.GaugeValue,
			float64(waterMarkSum),
			topic.Topic,
		)
	}

	return isOk
}
