package prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
	"strconv"
)

func (e *Exporter) collectConsumerGroups(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.ConsumerGroups.Enabled {
		return true
	}
	groups, err := e.minionSvc.DescribeConsumerGroups(ctx)
	if err != nil {
		e.logger.Error("failed to collect consumer groups, because Kafka request failed", zap.Error(err))
		return false
	}

	// iterate over responses of each broker
	for _, response := range groups.Groups {
		if response.Error != nil {
			e.logger.Warn("failed to describe consumer groups from one group coordinator",
				zap.Error(response.Error),
				zap.Int32("coordinator_id", response.BrokerMetadata.NodeID),
			)
			continue
		}
		coordinator := response.BrokerMetadata.NodeID
		// iterate over all groups coordinated by this broker
		for _, group := range response.Groups.Groups {
			err := kerr.ErrorForCode(group.ErrorCode)
			if err != nil {
				e.logger.Warn("failed to describe consumer group, internal kafka error",
					zap.Error(err),
					zap.String("group_id", group.Group),
				)
				continue
			}
			if !e.minionSvc.IsGroupAllowed(group.Group) {
				continue
			}
			state := 0
			if group.State == "Stable" {
				state = 1
			}
			ch <- prometheus.MustNewConstMetric(
				e.consumerGroupInfo,
				prometheus.GaugeValue,
				float64(state),
				group.Group,
				strconv.Itoa(len(group.Members)),
				group.Protocol,
				group.ProtocolType,
				group.State,
				strconv.FormatInt(int64(coordinator), 10),
			)
		}
	}
	return true
}
