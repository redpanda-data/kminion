package minion

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/cloudhut/kminion/v2/e2e"
	"github.com/cloudhut/kminion/v2/kafka"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

type Service struct {
	Cfg    Config
	logger *zap.Logger

	// requestGroup is used to deduplicate multiple concurrent requests to kafka
	requestGroup *singleflight.Group
	cache        map[string]interface{}
	cacheLock    sync.RWMutex

	AllowedGroupIDsExpr []*regexp.Regexp
	IgnoredGroupIDsExpr []*regexp.Regexp
	AllowedTopicsExpr   []*regexp.Regexp
	IgnoredTopicsExpr   []*regexp.Regexp

	kafkaSvc *kafka.Service
	storage  *Storage

	endToEndService *e2e.Service
}

func NewService(cfg Config, logger *zap.Logger, kafkaSvc *kafka.Service, metricNamespace string) (*Service, error) {
	storage, err := newStorage(logger)

	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// Compile regexes. We can ignore the errors because valid compilation has been validated already
	allowedGroupIDsExpr, _ := compileRegexes(cfg.ConsumerGroups.AllowedGroupIDs)
	ignoredGroupIDsExpr, _ := compileRegexes(cfg.ConsumerGroups.IgnoredGroupIDs)
	allowedTopicsExpr, _ := compileRegexes(cfg.Topics.AllowedTopics)
	ignoredTopicsExpr, _ := compileRegexes(cfg.Topics.IgnoredTopics)

	service := &Service{
		Cfg:    cfg,
		logger: logger,

		requestGroup: &singleflight.Group{},
		cache:        make(map[string]interface{}),
		cacheLock:    sync.RWMutex{},

		AllowedGroupIDsExpr: allowedGroupIDsExpr,
		IgnoredGroupIDsExpr: ignoredGroupIDsExpr,
		AllowedTopicsExpr:   allowedTopicsExpr,
		IgnoredTopicsExpr:   ignoredTopicsExpr,

		kafkaSvc: kafkaSvc,
		storage:  storage,

		endToEndService: nil,
	}

	// Create end-to-end service
	if cfg.EndToEnd.Enabled {
		service.endToEndService, err = e2e.NewService(
			cfg.EndToEnd,
			logger.With(zap.String("component", "endToEnd")),
			kafkaSvc,
			metricNamespace,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to create end-to-end monitoring service: %w", err)
		}
	}

	return service, nil
}

func (s *Service) Start(ctx context.Context) error {
	err := s.ensureCompatibility(ctx)
	if err != nil {
		return fmt.Errorf("failed to check feature compatibility against Kafka: %w", err)
	}

	if s.Cfg.ConsumerGroups.ScrapeMode == ConsumerGroupScrapeModeOffsetsTopic {
		go s.startConsumingOffsets(ctx)
	}

	if s.Cfg.EndToEnd.Enabled {
		go s.endToEndService.Start(ctx)
	}

	return nil
}

func (s *Service) ensureCompatibility(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	versionsRes, err := s.GetAPIVersions(ctx)
	if err != nil {
		return fmt.Errorf("kafka api versions couldn't be fetched: %w", err)
	}
	versions := kversion.FromApiVersionsResponse(versionsRes)

	// Check Describe Log Dirs
	if s.Cfg.LogDirs.Enabled {
		k := kmsg.NewDescribeLogDirsRequest()
		isSupported := versions.HasKey(k.Key())
		if !isSupported {
			s.logger.Warn("describing log dirs is enabled, but it is not supported because your Kafka cluster " +
				"version is too old. feature will be disabled")
			s.Cfg.LogDirs.Enabled = false
		}
	}

	return nil
}

func (s *Service) getCachedItem(key string) (interface{}, bool) {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	val, exists := s.cache[key]
	return val, exists
}

func (s *Service) setCachedItem(key string, val interface{}, timeout time.Duration) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	go func() {
		time.Sleep(timeout)
		s.deleteCachedItem(key)
	}()

	s.cache[key] = val
}

func (s *Service) deleteCachedItem(key string) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	delete(s.cache, key)
}
