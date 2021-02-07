package minion

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/sync/errgroup"
	"sync"
)

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
	// TODO: Filter consumer groups

	return res, nil
}

func (s *Service) DescribeConsumerGroups(ctx context.Context) (*kmsg.DescribeGroupsResponse, error) {
	listRes, err := s.listConsumerGroups(ctx)
	if err != nil {
		return nil, err
	}

	groupIDs := make([]string, len(listRes.Groups))
	for i, group := range listRes.Groups {
		groupIDs[i] = group.Group
	}

	describeReq := kmsg.NewDescribeGroupsRequest()
	describeReq.Groups = groupIDs
	describeRes, err := describeReq.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer groups: %w", err)
	}

	return describeRes, err
}

// ListConsumerGroupOffsets returns the committed group offsets for a single group
func (s *Service) ListConsumerGroupOffsets(ctx context.Context, group string) (*kmsg.OffsetFetchResponse, error) {
	req := kmsg.NewOffsetFetchRequest()
	req.Group = group
	req.Topics = nil
	res, err := req.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to request group offsets for group '%v': %w", group, err)
	}

	err = kerr.ErrorForCode(res.ErrorCode)
	if err != nil {
		return nil, fmt.Errorf("failed to request group offsets for group '%v'. inner error: %w", group, err)
	}

	return res, nil
}

// ListConsumerGroupOffsetsBulk returns a map which has the Consumer group name as key
func (s *Service) ListConsumerGroupOffsetsBulk(ctx context.Context, groups []string) (map[string]*kmsg.OffsetFetchResponse, error) {
	eg, _ := errgroup.WithContext(ctx)

	mutex := sync.Mutex{}
	res := make(map[string]*kmsg.OffsetFetchResponse)

	f := func(group string) func() error {
		return func() error {
			offsets, err := s.ListConsumerGroupOffsets(ctx, group)
			if err != nil {
				return err
			}

			mutex.Lock()
			res[group] = offsets
			mutex.Unlock()
			return nil
		}
	}

	for _, group := range groups {
		eg.Go(f(group))
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}
