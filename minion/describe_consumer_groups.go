package minion

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// Group types as reported by the ListGroups response (Kafka 3.8+). Groups using the consumer rebalance
// protocol introduced by KIP-848 cannot be described with the classic DescribeGroups API, which answers
// GROUP_ID_NOT_FOUND for them, so they have to be described with ConsumerGroupDescribe instead.
const (
	groupTypeClassic  = "classic"
	groupTypeConsumer = "consumer"
)

type DescribeConsumerGroupsResponse struct {
	BrokerMetadata kgo.BrokerMetadata
	Groups         *kmsg.DescribeGroupsResponse
}

func (s *Service) listConsumerGroupsCached(ctx context.Context) (*kmsg.ListGroupsResponse, error) {
	reqId := ctx.Value("requestId").(string)
	key := "list-consumer-groups-" + reqId

	if cachedRes, exists := s.getCachedItem(key); exists {
		return cachedRes.(*kmsg.ListGroupsResponse), nil
	}
	res, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		res, err := s.listConsumerGroups(ctx)
		if err != nil {
			return nil, err
		}
		s.setCachedItem(key, res, 120*time.Second)

		return res, nil
	})
	if err != nil {
		return nil, err
	}

	return res.(*kmsg.ListGroupsResponse), nil
}

func (s *Service) listConsumerGroups(ctx context.Context) (*kmsg.ListGroupsResponse, error) {
	listReq := kmsg.NewListGroupsRequest()
	res, err := listReq.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	err = kerr.ErrorForCode(res.ErrorCode)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups. inner kafka error: %w", err)
	}

	return res, nil
}

func (s *Service) DescribeConsumerGroups(ctx context.Context) ([]DescribeConsumerGroupsResponse, error) {
	listRes, err := s.listConsumerGroupsCached(ctx)
	if err != nil {
		return nil, err
	}

	classicGroupIDs, consumerGroupIDs := partitionGroupIDsByType(listRes)

	describedGroups := s.describeClassicGroups(ctx, classicGroupIDs)
	describedGroups = append(describedGroups, s.describeConsumerProtocolGroups(ctx, consumerGroupIDs)...)

	return describedGroups, nil
}

// partitionGroupIDsByType splits the listed groups into the ones using the classic rebalance protocol and
// the ones using the consumer rebalance protocol. The group type is only reported by ListGroups v5+. Brokers
// that do not report it cannot run the consumer rebalance protocol either, so treating those groups as
// classic ones is always correct.
func partitionGroupIDsByType(listRes *kmsg.ListGroupsResponse) (classicGroupIDs []string, consumerGroupIDs []string) {
	classicGroupIDs = make([]string, 0, len(listRes.Groups))
	consumerGroupIDs = make([]string, 0)
	for _, group := range listRes.Groups {
		if strings.EqualFold(group.GroupType, groupTypeConsumer) {
			consumerGroupIDs = append(consumerGroupIDs, group.Group)
			continue
		}
		classicGroupIDs = append(classicGroupIDs, group.Group)
	}

	return classicGroupIDs, consumerGroupIDs
}

// describeClassicGroups describes groups using the classic rebalance protocol via the DescribeGroups API.
func (s *Service) describeClassicGroups(ctx context.Context, groupIDs []string) []DescribeConsumerGroupsResponse {
	if len(groupIDs) == 0 {
		return make([]DescribeConsumerGroupsResponse, 0)
	}

	describeReq := kmsg.NewDescribeGroupsRequest()
	describeReq.Groups = groupIDs
	describeReq.IncludeAuthorizedOperations = false
	shardedResp := s.client.RequestSharded(ctx, &describeReq)

	describedGroups := make([]DescribeConsumerGroupsResponse, 0, len(shardedResp))
	for _, kresp := range shardedResp {
		if kresp.Err != nil {
			s.logger.Warn("broker failed to respond to the described groups request",
				zap.Int32("broker_id", kresp.Meta.NodeID),
				zap.Error(kresp.Err))
			continue
		}
		res := kresp.Resp.(*kmsg.DescribeGroupsResponse)

		describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
			BrokerMetadata: kresp.Meta,
			Groups:         res,
		})
	}

	return describedGroups
}

// describeConsumerProtocolGroups describes groups using the consumer rebalance protocol (KIP-848) via the
// ConsumerGroupDescribe API. The responses are converted into DescribeGroups responses so that both group
// types can be exported through the same code path.
func (s *Service) describeConsumerProtocolGroups(ctx context.Context, groupIDs []string) []DescribeConsumerGroupsResponse {
	if len(groupIDs) == 0 {
		return make([]DescribeConsumerGroupsResponse, 0)
	}

	describeReq := kmsg.NewConsumerGroupDescribeRequest()
	describeReq.Groups = groupIDs
	describeReq.IncludeAuthorizedOperations = false
	shardedResp := s.client.RequestSharded(ctx, &describeReq)

	describedGroups := make([]DescribeConsumerGroupsResponse, 0, len(shardedResp))
	for _, kresp := range shardedResp {
		if kresp.Err != nil {
			s.logger.Warn("broker failed to respond to the consumer group describe request",
				zap.Int32("broker_id", kresp.Meta.NodeID),
				zap.Error(kresp.Err))
			continue
		}
		res := kresp.Resp.(*kmsg.ConsumerGroupDescribeResponse)

		describedGroups = append(describedGroups, DescribeConsumerGroupsResponse{
			BrokerMetadata: kresp.Meta,
			Groups:         consumerGroupDescribeToDescribeGroups(res),
		})
	}

	return describedGroups
}

// consumerGroupDescribeToDescribeGroups converts a ConsumerGroupDescribe response into the equivalent
// DescribeGroups response. Member assignments are re-encoded into the classic wire format so that consumers
// of the response can decode them the same way regardless of the rebalance protocol the group uses.
func consumerGroupDescribeToDescribeGroups(res *kmsg.ConsumerGroupDescribeResponse) *kmsg.DescribeGroupsResponse {
	converted := kmsg.NewPtrDescribeGroupsResponse()
	converted.Version = res.Version
	converted.ThrottleMillis = res.ThrottleMillis
	converted.Groups = make([]kmsg.DescribeGroupsResponseGroup, 0, len(res.Groups))

	for _, group := range res.Groups {
		convertedGroup := kmsg.NewDescribeGroupsResponseGroup()
		convertedGroup.ErrorCode = group.ErrorCode
		convertedGroup.Group = group.Group
		convertedGroup.State = group.State
		// The consumer rebalance protocol has no notion of a protocol type, but every group using it is a
		// consumer group. Reporting it as such keeps the exported labels stable across both protocols.
		convertedGroup.ProtocolType = groupTypeConsumer
		convertedGroup.Protocol = group.AssignorName
		convertedGroup.AuthorizedOperations = group.AuthorizedOperations
		convertedGroup.Members = make([]kmsg.DescribeGroupsResponseGroupMember, 0, len(group.Members))

		for _, member := range group.Members {
			convertedMember := kmsg.NewDescribeGroupsResponseGroupMember()
			convertedMember.MemberID = member.MemberID
			convertedMember.InstanceID = member.InstanceID
			convertedMember.ClientID = member.ClientID
			convertedMember.ClientHost = member.ClientHost
			convertedMember.MemberAssignment = encodeMemberAssignment(member.Assignment)

			convertedGroup.Members = append(convertedGroup.Members, convertedMember)
		}

		converted.Groups = append(converted.Groups, convertedGroup)
	}

	return converted
}

// encodeMemberAssignment encodes a consumer rebalance protocol assignment into the classic member assignment
// wire format. An assignment without any topic partitions is encoded as no assignment at all, which is how
// the classic protocol reports members that have not been assigned anything yet.
func encodeMemberAssignment(assignment kmsg.Assignment) []byte {
	if len(assignment.TopicPartitions) == 0 {
		return nil
	}

	kassignment := kmsg.NewConsumerMemberAssignment()
	kassignment.Topics = make([]kmsg.ConsumerMemberAssignmentTopic, 0, len(assignment.TopicPartitions))
	for _, topicPartition := range assignment.TopicPartitions {
		topic := kmsg.NewConsumerMemberAssignmentTopic()
		topic.Topic = topicPartition.Topic
		topic.Partitions = topicPartition.Partitions

		kassignment.Topics = append(kassignment.Topics, topic)
	}

	return kassignment.AppendTo(nil)
}
