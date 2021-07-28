package minion

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// ListAllConsumerGroupOffsetsInternal returns a map from the in memory storage. The map value is the offset commit
// value and is grouped by group id, topic, partition id as keys of the nested maps.
func (s *Service) ListAllConsumerGroupOffsetsInternal() map[string]map[string]map[int32]OffsetCommit {
	return s.storage.getGroupOffsets()
}

// ListAllConsumerGroupOffsetsAdminAPI return all consumer group offsets using Kafka's Admin API.
func (s *Service) ListAllConsumerGroupOffsetsAdminAPI(ctx context.Context) (map[string]*kmsg.OffsetFetchResponse, error) {
	groupsRes, err := s.listConsumerGroupsCached(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list groupsRes: %w", err)
	}
	groupIDs := make([]string, len(groupsRes.Groups))
	for i, group := range groupsRes.Groups {
		groupIDs[i] = group.Group
	}

	return s.listConsumerGroupOffsetsBulk(ctx, groupIDs)
}

// listConsumerGroupOffsetsBulk returns a map which has the Consumer group name as key
func (s *Service) listConsumerGroupOffsetsBulk(ctx context.Context, groups []string) (map[string]*kmsg.OffsetFetchResponse, error) {
	eg, _ := errgroup.WithContext(ctx)

	mutex := sync.Mutex{}
	res := make(map[string]*kmsg.OffsetFetchResponse)

	f := func(group string) func() error {
		return func() error {
			offsets, err := s.listConsumerGroupOffsets(ctx, group)
			if err != nil {
				s.logger.Warn("failed to fetch consumer group offsets, inner kafka error",
					zap.String("consumer_group", group),
					zap.Error(err))
				return nil
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

// listConsumerGroupOffsets returns the committed group offsets for a single group
func (s *Service) listConsumerGroupOffsets(ctx context.Context, group string) (*kmsg.OffsetFetchResponse, error) {
	req := kmsg.NewOffsetFetchRequest()
	req.Group = group
	req.Topics = nil
	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to request group offsets for group '%v': %w", group, err)
	}

	return res, nil
}
