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

	// The list of groups may be incomplete due to group coordinators that might fail to respond. We do log a error
	// message in that case (in the kafka request method) and groups will not be included in this list.
	for _, grp := range groups {
		coordinator := grp.BrokerMetadata.NodeID
		for _, group := range grp.Groups.Groups {
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
