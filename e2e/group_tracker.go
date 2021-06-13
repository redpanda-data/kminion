package e2e

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

const (
	oldGroupCheckInterval = 5 * time.Second  // how often to check for old kminion groups
	oldGroupMaxAge        = 20 * time.Second // maximum age after which an old group should be deleted
)

// groupTracker keeps checking for empty consumerGroups matching the kminion prefix.
// When a group was seen empty for some time, we delete it.
// Why?
// Whenever a kminion instance starts up it creates a consumer-group for itself in order to not "collide" with other kminion instances.
// When an instance restarts (for whatever reason), it creates a new group again, so we'd end up with a lot of unused groups.
type groupTracker struct {
	cfg                    Config
	logger                 *zap.Logger
	client                 *kgo.Client          // kafka client
	groupId                string               // our own groupId
	potentiallyEmptyGroups map[string]time.Time // groupName -> utc timestamp when the group was first seen
}

func newGroupTracker(cfg Config, logger *zap.Logger, client *kgo.Client, groupID string) *groupTracker {
	return &groupTracker{
		cfg:                    cfg,
		logger:                 logger.Named("groupTracker"),
		client:                 client,
		groupId:                groupID,
		potentiallyEmptyGroups: make(map[string]time.Time),
	}
}

func (g *groupTracker) start(ctx context.Context) {
	g.logger.Debug("starting group tracker")

	deleteOldGroupsTicker := time.NewTicker(oldGroupCheckInterval)
	for {
		select {
		case <-ctx.Done():
			g.logger.Debug("stopping group tracker, context was cancelled")
			return
		case <-deleteOldGroupsTicker.C:
			childCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := g.checkAndDeleteOldConsumerGroups(childCtx)
			if err != nil {
				g.logger.Error("failed to check for old consumer groups: %w", zap.Error(err))
			}
			cancel()
		}
	}
}

func (g *groupTracker) checkAndDeleteOldConsumerGroups(ctx context.Context) error {
	groupsRq := kmsg.NewListGroupsRequest()
	groupsRq.StatesFilter = []string{"Empty"}

	g.logger.Debug("checking for stale kminion consumer groups")

	shardedResponse := g.client.RequestSharded(ctx, &groupsRq)

	// find groups that start with the kminion prefix
	matchingGroups := make([]string, 0)
	for _, shard := range shardedResponse {
		if shard.Err != nil {
			g.logger.Error("error in response to ListGroupsRequest", zap.Int32("broker_id", shard.Meta.NodeID), zap.Error(shard.Err))
			continue
		}

		r, ok := shard.Resp.(*kmsg.ListGroupsResponse)
		if !ok {
			g.logger.Error("cannot cast responseShard.Resp to kmsg.ListGroupsResponse")
			continue
		}

		for _, group := range r.Groups {
			name := group.Group

			if name == g.groupId {
				continue // skip our own consumer group
			}

			if strings.HasPrefix(name, g.cfg.Consumer.GroupIdPrefix) {
				matchingGroups = append(matchingGroups, name)
			}
		}
	}

	// save new (previously unseen) groups to tracker
	g.logger.Debug("checked for stale consumer groups", zap.Int("found_groups", len(matchingGroups)), zap.Strings("groups", matchingGroups))
	for _, name := range matchingGroups {
		_, exists := g.potentiallyEmptyGroups[name]
		if !exists {
			// add it with the current timestamp
			g.potentiallyEmptyGroups[name] = time.Now()
			g.logger.Debug("found new empty kminion group, adding it to the tracker", zap.String("group", name))
		}
	}

	// go through saved groups:
	// - don't track the ones we don't see anymore (bc they got deleted or are not empty anymore)
	// - mark the ones that are too old (have been observed as empty for too long)
	groupsToDelete := make([]string, 0)
	for name, firstSeen := range g.potentiallyEmptyGroups {
		exists, _ := containsStr(matchingGroups, name)
		if exists {
			// still there, check age and maybe delete it
			age := time.Since(firstSeen)
			if age > oldGroupMaxAge {
				// group was unused for too long, delete it
				groupsToDelete = append(groupsToDelete, name)
				delete(g.potentiallyEmptyGroups, name)
			}
		} else {
			// does not exist anymore, it must have been deleted, or is in use now (no longer empty)
			// don't track it anymore
			delete(g.potentiallyEmptyGroups, name)
		}
	}

	// actually delete the groups we've decided to delete
	if len(groupsToDelete) == 0 {
		return nil
	}

	deleteRq := kmsg.NewDeleteGroupsRequest()
	deleteRq.Groups = groupsToDelete
	deleteResp := g.client.RequestSharded(ctx, &deleteRq)

	// done, now just errors
	// if we get a not authorized error we'll disable deleting groups
	foundNotAuthorizedError := false
	deletedGroups := make([]string, 0)
	for _, shard := range deleteResp {
		if shard.Err != nil {
			g.logger.Error("sharded consumer group delete request failed", zap.Error(shard.Err))
			continue
		}

		resp, ok := shard.Resp.(*kmsg.DeleteGroupsResponse)
		if !ok {
			g.logger.Error("failed to cast shard response to DeleteGroupsResponse while handling an error for deleting groups", zap.String("shardHost", shard.Meta.Host), zap.Int32("broker_id", shard.Meta.NodeID), zap.NamedError("shardError", shard.Err))
			continue
		}

		for _, groupResp := range resp.Groups {
			err := kerr.ErrorForCode(groupResp.ErrorCode)
			if err != nil {
				g.logger.Error("failed to delete consumer group", zap.String("shard", shard.Meta.Host), zap.Int32("broker_id", shard.Meta.NodeID), zap.String("group", groupResp.Group), zap.Error(err))

				if groupResp.ErrorCode == kerr.GroupAuthorizationFailed.Code {
					foundNotAuthorizedError = true
				}

			} else {
				deletedGroups = append(deletedGroups, groupResp.Group)
			}
		}
	}
	g.logger.Info("deleted old consumer groups", zap.Strings("deletedGroups", deletedGroups))

	if foundNotAuthorizedError {
		g.logger.Info("disabling trying to delete old kminion consumer-groups since one of the last delete results had an 'GroupAuthorizationFailed' error")
	}

	return nil
}
