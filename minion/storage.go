package minion

import (
	"fmt"
	"strconv"
	"time"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Storage stores the current state of all consumer group information that has been consumed using the offset consumer.
type Storage struct {
	logger *zap.Logger

	// offsetCommits is a map of all consumer offsets.
	// A unique key in the format "group:topic:partition" is used as map key.
	// Value is of type OffsetCommit
	offsetCommits cmap.ConcurrentMap

	// progressTracker is a map that tracks what offsets in each partition have already been consumed
	progressTracker cmap.ConcurrentMap

	isReadyBool *atomic.Bool

	// Number of consumed records (used for a Prometheus metric)
	consumedRecords *atomic.Float64
}

// OffsetCommit is used as value for the OffsetCommit map
type OffsetCommit struct {
	Key   kmsg.OffsetCommitKey
	Value kmsg.OffsetCommitValue

	// CommitCount is the number of offset commits for this group-topic-partition combination
	CommitCount int

	// ExpireTimestamp is a timestamp that indicates when this offset commit will expire on the Kafka cluster
	ExpireTimestamp time.Time
}

func newStorage(logger *zap.Logger) (*Storage, error) {
	return &Storage{
		logger:          logger.Named("storage"),
		offsetCommits:   cmap.New(),
		progressTracker: cmap.New(),
		isReadyBool:     atomic.NewBool(false),
		consumedRecords: atomic.NewFloat64(0),
	}, nil
}

func (s *Storage) isReady() bool {
	return s.isReadyBool.Load()
}

func (s *Storage) setReadyState(isReady bool) {
	s.isReadyBool.Store(isReady)
}

// markRecordConsumed stores the latest consumed offset for each partition. This is necessary in order to figure out
// whether we have caught up the message lag when starting KMinion as we start consuming from the very oldest offset
// commit.
func (s *Storage) markRecordConsumed(rec *kgo.Record) {
	key := fmt.Sprintf("%v", rec.Partition)
	s.progressTracker.Set(key, rec.Offset)
	s.consumedRecords.Add(1)
}

func (s *Storage) addOffsetCommit(key kmsg.OffsetCommitKey, value kmsg.OffsetCommitValue) {
	// For performance reasons we'll store offset commits using a "unique key". Writes happen way more frequently than
	// reads (Prometheus scraping the endpoint). Hence we can group everything by group or topic on the read path as
	// needed instead of writing it into nested maps like a map[GroupID]map[Topic]map[Partition]
	uniqueKey := encodeOffsetCommitKey(key)

	commitCount := 0
	commitInterface, exists := s.offsetCommits.Get(uniqueKey)
	if exists {
		offsetCommit := commitInterface.(OffsetCommit)
		commitCount = offsetCommit.CommitCount
	}

	timeDay := 24 * time.Hour
	commit := OffsetCommit{
		Key:             key,
		Value:           value,
		CommitCount:     commitCount + 1,
		ExpireTimestamp: time.Unix(0, value.CommitTimestamp*int64(time.Millisecond)).Add(7 * timeDay),
	}
	s.offsetCommits.Set(uniqueKey, commit)
}

func (s *Storage) getConsumedOffsets() map[int32]int64 {
	offsetsByPartition := make(map[int32]int64)
	offsets := s.progressTracker.Items()
	for partitionID, offsetStr := range offsets {
		val := offsetStr.(int64)
		partitionID, _ := strconv.ParseInt(partitionID, 10, 32)
		offsetsByPartition[int32(partitionID)] = val
	}

	return offsetsByPartition
}

func (s *Storage) getNumberOfConsumedRecords() float64 {
	return s.consumedRecords.Load()
}

func (s *Storage) getGroupOffsets(isAllowed func(groupName string) bool) map[string]map[string]map[int32]OffsetCommit {
	// Offsets by group, topic, partition
	offsetsByGroup := make(map[string]map[string]map[int32]OffsetCommit)

	if !s.isReady() {
		s.logger.Info("Tried to fetch consumer group offsets, but haven't consumed the whole topic yet")
		return offsetsByGroup
	}

	offsets := s.offsetCommits.Items()
	for _, offset := range offsets {
		val := offset.(OffsetCommit)

		if !isAllowed(val.Key.Group) {
			continue
		}

		// Initialize inner maps as necessary
		if _, exists := offsetsByGroup[val.Key.Group]; !exists {
			offsetsByGroup[val.Key.Group] = make(map[string]map[int32]OffsetCommit)
		}
		if _, exists := offsetsByGroup[val.Key.Group][val.Key.Topic]; !exists {
			offsetsByGroup[val.Key.Group][val.Key.Topic] = make(map[int32]OffsetCommit)
		}

		offsetsByGroup[val.Key.Group][val.Key.Topic][val.Key.Partition] = val
	}

	return offsetsByGroup
}

func (s *Storage) deleteOffsetCommit(key kmsg.OffsetCommitKey) {
	uniqueKey := encodeOffsetCommitKey(key)
	s.offsetCommits.Remove(uniqueKey)
}

func encodeOffsetCommitKey(key kmsg.OffsetCommitKey) string {
	return fmt.Sprintf("%v:%v:%v", key.Group, key.Topic, key.Partition)
}
