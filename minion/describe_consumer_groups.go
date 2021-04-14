package minion

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"time"
)

type DescribeConsumerGroupsResponseSharded struct {
	Groups         []DescribeConsumerGroupsResponse
	RequestsSent   int
	RequestsFailed int
}

type DescribeConsumerGroupsResponse struct {
	BrokerMetadata kgo.BrokerMetadata
	Groups         *kmsg.DescribeGroupsResponse
	Error          error
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
	res, err := listReq.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	err = kerr.ErrorForCode(res.ErrorCode)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups. inner kafka error: %w", err)
	}

	return res, nil
}

func (s *Service) DescribeConsumerGroups(ctx context.Context) (*DescribeConsumerGroupsResponseSharded, error) {
	var listRes, err = s.listConsumerGroupsCached(ctx)
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
	shardedResp := s.kafkaSvc.Client.RequestSharded(ctx, &describeReq)
	describeRes := &DescribeConsumerGroupsResponseSharded{
		Groups:         make([]DescribeConsumerGroupsResponse, 0),
		RequestsSent:   0,
		RequestsFailed: 0,
	}
	var kErr error
	for _, kresp := range shardedResp {
		describeRes.RequestsSent++
		if kresp.Err != nil {
			describeRes.RequestsFailed++
			kErr = kresp.Err
		}
		res := kresp.Resp.(*kmsg.DescribeGroupsResponse)

		describeRes.Groups = append(describeRes.Groups, DescribeConsumerGroupsResponse{
			BrokerMetadata: kresp.Meta,
			Groups:         res,
			Error:          kresp.Err,
		})
	}
	if describeRes.RequestsSent > 0 && describeRes.RequestsSent == describeRes.RequestsFailed {
		return describeRes, fmt.Errorf("all '%v' requests have failed, last error: %w", len(shardedResp), kErr)
	}
	return describeRes, nil
}
