package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectTopicInfo(ctx context.Context, ch chan<- prometheus.Metric) bool {
	metadata, err := e.minionSvc.GetMetadataCached(ctx)
	if err != nil {
		e.logger.Error("failed to get metadata", zap.Error(err))
		return false
	}

	topicConfigs, err := e.minionSvc.GetTopicConfigs(ctx)
	if err != nil {
		e.logger.Error("failed to get topic configs", zap.Error(err))
		return false
	}

	isOk := true
	// ConfigsByTopic is indexed by topic name and config resource name (inner key)
	configsByTopic := make(map[string]map[string]string)
	for _, resource := range topicConfigs.Resources {
		configsByTopic[resource.ResourceName] = make(map[string]string)
		typedErr := kerr.TypedErrorForCode(resource.ErrorCode)
		if typedErr != nil {
			isOk = false
			e.logger.Warn("failed to get topic config of a specific topic",
				zap.String("topic_name", resource.ResourceName),
				zap.Error(typedErr))
			continue
		}

		for _, config := range resource.Configs {
			confVal := "nil"
			if config.Value != nil {
				confVal = *config.Value
			}
			configsByTopic[resource.ResourceName][config.Name] = confVal
		}

	}

	for _, topic := range metadata.Topics {
		if !e.minionSvc.IsTopicAllowed(topic.Topic) {
			continue
		}
		typedErr := kerr.TypedErrorForCode(topic.ErrorCode)
		if typedErr != nil {
			isOk = false
			e.logger.Warn("failed to get metadata of a specific topic",
				zap.String("topic_name", topic.Topic),
				zap.Error(typedErr))
			return false
		}
		partitionCount := len(topic.Partitions)
		replicationFactor := -1
		if partitionCount > 0 {
			// It should never be possible to skip this, but just to be safe we'll check this so that we don't cause panics
			replicationFactor = len(topic.Partitions[0].Replicas)
		}

		cleanupPolicy, exists := configsByTopic[topic.Topic]["cleanup.policy"]
		if !exists {
			cleanupPolicy = "N/A"
		}

		ch <- prometheus.MustNewConstMetric(
			e.topicInfo,
			prometheus.GaugeValue,
			float64(1),
			topic.Topic,
			strconv.Itoa(partitionCount),
			strconv.Itoa(replicationFactor),
			cleanupPolicy,
		)
	}
	return isOk
}
