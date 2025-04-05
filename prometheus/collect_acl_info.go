package prometheus

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (e *Exporter) collectACLInfo(ctx context.Context, ch chan<- prometheus.Metric) bool {
	ACLRes, err := e.minionSvc.ListAllACLs(ctx)
	if err != nil {
		e.logger.Error("failed to fetch ACLs", zap.Error(err))
		return false
	}

	ACLsByType := getResourceTypeName(ACLRes)
	totalACLs := 0
	for _, count := range ACLsByType {
		totalACLs += count
	}

	ch <- prometheus.MustNewConstMetric(
		e.aclCount,
		prometheus.GaugeValue,
		float64(totalACLs),
	)

	for resourceType, count := range ACLsByType {
		ch <- prometheus.MustNewConstMetric(
			e.aclCountByType,
			prometheus.GaugeValue,
			float64(count),
			resourceType,
		)
	}

	return true
}

func getResourceTypeName(ACLResponse *kmsg.DescribeACLsResponse) map[string]int {
	ACLsByType := make(map[string]int)
	for _, resource := range ACLResponse.Resources {
		resourceType := "unknown"
		switch resource.ResourceType {
		case 0:
			resourceType = "unknown"
		case 1:
			resourceType = "any"
		case 2:
			resourceType = "topic"
		case 3:
			resourceType = "group"
		case 4:
			resourceType = "cluster"
		case 5:
			resourceType = "transactional_id"
		case 6:
			resourceType = "delegation_token"
		case 7:
			resourceType = "user"
		}

		ACLsByType[resourceType] += len(resource.ACLs)
	}

	return ACLsByType
}
