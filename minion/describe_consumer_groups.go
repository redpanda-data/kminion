package minion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
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
		allowedGroups := make([]kmsg.ListGroupsResponseGroup, 0)
		for i := range res.Groups {
			if s.IsGroupAllowed(res.Groups[i].Group) {
				allowedGroups = append(allowedGroups, res.Groups[i])
			}
		}
		res.Groups = allowedGroups
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

	groupIDs := make([]string, len(listRes.Groups))
	for i, group := range listRes.Groups {
		groupIDs[i] = group.Group
	}

	describeReq := kmsg.NewDescribeGroupsRequest()
	describeReq.Groups = groupIDs
	describeReq.IncludeAuthorizedOperations = false
	shardedResp := s.client.RequestSharded(ctx, &describeReq)

	describedGroups := make([]DescribeConsumerGroupsResponse, 0)
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

	return describedGroups, nil
}
