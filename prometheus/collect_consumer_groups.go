package prometheus

import (
	"context"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (e *Exporter) collectConsumerGroups(ctx context.Context, ch chan<- prometheus.Metric) bool {
	if !e.minionSvc.Cfg.ConsumerGroups.Enabled {
		return true
	}
	responseShards, err := e.minionSvc.DescribeConsumerGroups(ctx)
	if err != nil {
		e.logger.Error("failed to collect consumer responseShards, because Kafka request failed", zap.Error(err))
		return false
	}
	e.logger.Info("described consumer group shards", zap.Int("shard_count", len(responseShards)))

	// The list of responseShards may be incomplete due to group coordinators that might fail to respond. We do log an error
	// message in that case (in the kafka request method) and responseShards will not be included in this list.
	for shardIndex, shard := range responseShards {
		e.logger.Info("described consumer responseShards on shard",
			zap.Int("shard_index", shardIndex),
			zap.Int32("coordinator_id", shard.BrokerMetadata.NodeID),
			zap.Int("group_count", len(shard.Groups.Groups)))
		coordinator := shard.BrokerMetadata.NodeID
		for _, group := range shard.Groups.Groups {
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

			e.logger.Info("iterated on described consumer responseShards",
				zap.String("group", group.Group),
				zap.String("protoocl", group.Protocol),
				zap.String("state", group.State),
				zap.Int("members", len(group.Members)),
				zap.Int("shard_index", shardIndex),
			)
			ch <- prometheus.MustNewConstMetric(
				e.consumerGroupInfo,
				prometheus.GaugeValue,
				float64(state),
				group.Group,
				group.Protocol,
				group.ProtocolType,
				group.State,
				strconv.FormatInt(int64(coordinator), 10),
			)

			// total number of members in consumer responseShards
			ch <- prometheus.MustNewConstMetric(
				e.consumerGroupMembers,
				prometheus.GaugeValue,
				float64(len(group.Members)),
				group.Group,
			)

			// iterate all members and build two maps:
			// - {topic -> number-of-consumers}
			// - {topic -> number-of-partitions-assigned}
			topicConsumers := make(map[string]int)
			topicPartitionsAssigned := make(map[string]int)
			membersWithEmptyAssignment := 0
			failedAssignmentsDecode := 0
			for _, member := range group.Members {
				kassignment, err := decodeMemberAssignments(group.ProtocolType, member)
				if err != nil {
					e.logger.Debug("failed to decode consumer group member assignment, internal kafka error",
						zap.Error(err),
						zap.String("group_id", group.Group),
						zap.String("client_id", member.ClientID),
						zap.String("member_id", member.MemberID),
						zap.String("client_host", member.ClientHost),
					)
					failedAssignmentsDecode++
					continue
				}
				if kassignment == nil {
					// This is expected in the case of protocolTypes that don't provide valuable information
					continue
				}

				if len(kassignment.Topics) == 0 {
					membersWithEmptyAssignment++
				}
				for _, topic := range kassignment.Topics {
					topicConsumers[topic.Topic]++
					topicPartitionsAssigned[topic.Topic] += len(topic.Partitions)
				}
			}

			if failedAssignmentsDecode > 0 {
				e.logger.Error("failed to decode consumer group member assignment, internal kafka error",
					zap.Error(err),
					zap.String("group_id", group.Group),
					zap.Int("assignment_decode_failures", failedAssignmentsDecode),
				)
			}

			// number of members with no assignment in a stable consumer group
			if membersWithEmptyAssignment > 0 {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupMembersEmpty,
					prometheus.GaugeValue,
					float64(membersWithEmptyAssignment),
					group.Group,
				)
			}
			// number of members in consumer responseShards for each topic
			for topicName, consumers := range topicConsumers {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupTopicMembers,
					prometheus.GaugeValue,
					float64(consumers),
					group.Group,
					topicName,
				)
			}
			// number of partitions assigned in consumer responseShards for each topic
			for topicName, partitions := range topicPartitionsAssigned {
				ch <- prometheus.MustNewConstMetric(
					e.consumerGroupAssignedTopicPartitions,
					prometheus.GaugeValue,
					float64(partitions),
					group.Group,
					topicName,
				)
			}
		}
	}
	return true
}

func decodeMemberAssignments(protocolType string, member kmsg.DescribeGroupsResponseGroupMember) (*kmsg.ConsumerMemberAssignment, error) {
	switch protocolType {
	case "consumer":
		a := kmsg.NewConsumerMemberAssignment()
		if err := a.ReadFrom(member.MemberAssignment); err != nil {
			return nil, fmt.Errorf("failed to decode member assignment: %w", err)
		}
		return &a, nil
	case "connect":
		return nil, nil
	default:
		return nil, nil
	}
}
