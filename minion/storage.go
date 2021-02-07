package minion

import (
	"fmt"
	"github.com/orcaman/concurrent-map"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// Storage stores the current state
type Storage struct {
	logger *zap.Logger

	// offsetCommits is a map of all consumer offsets. A unique key in the format "group:topic:partition" is used as map key.
	offsetCommits cmap.ConcurrentMap
}

// offsetCommit is used as value for the offsetCommit map
type offsetCommit struct {
	kmsg.OffsetCommitKey
	kmsg.OffsetCommitValue
}

func newStorage(logger *zap.Logger) (*Storage, error) {
	return &Storage{
		logger:        logger,
		offsetCommits: cmap.New(),
	}, nil
}

func (s *Storage) addOffsetCommit(key kmsg.OffsetCommitKey, value kmsg.OffsetCommitValue) {
	// For performance reasons we'll store offset commits using a "unique key". Writes happen way more frequently than
	// reads (Prometheus scraping the endpoint). Hence we can group everything by group or topic on the read path as
	// needed instead of writing it into nested maps like a map[GroupID]map[Topic]map[Partition]
	uniqueKey := fmt.Sprintf("%v:%v:%v", key.Group, key.Topic, key.Partition)
	commit := offsetCommit{
		OffsetCommitKey:   key,
		OffsetCommitValue: value,
	}
	s.offsetCommits.Set(uniqueKey, commit)
}
