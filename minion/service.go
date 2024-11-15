package minion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/cloudhut/kminion/v2/kafka"
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

	client    *kgo.Client
	admClient *kadm.Client
	storage   *Storage
}

func NewService(cfg Config, logger *zap.Logger, kafkaSvc *kafka.Service, metricsNamespace string, ctx context.Context) (*Service, error) {
	storage, err := newStorage(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// Kafka client
	minionHooks := newMinionClientHooks(logger.Named("kafka_hooks"), metricsNamespace)
	kgoOpts := []kgo.Opt{
		kgo.WithHooks(minionHooks),
	}
	if cfg.ConsumerGroups.Enabled && cfg.ConsumerGroups.ScrapeMode == ConsumerGroupScrapeModeOffsetsTopic {
		kgoOpts = append(kgoOpts,
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.ConsumeTopics("__consumer_offsets"))
	}

	logger.Info("connecting to Kafka seed brokers, trying to fetch cluster metadata",
		zap.String("seed_brokers", strings.Join(kafkaSvc.Brokers(), ",")))

	client, err := kafkaSvc.CreateAndTestClient(ctx, logger, kgoOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}
	logger.Info("successfully connected to kafka cluster")

	// Compile regexes. We can ignore the errors because valid compilation has been validated already
	allowedGroupIDsExpr, _ := compileRegexes(cfg.ConsumerGroups.AllowedGroupIDs)
	ignoredGroupIDsExpr, _ := compileRegexes(cfg.ConsumerGroups.IgnoredGroupIDs)
	allowedTopicsExpr, _ := compileRegexes(cfg.Topics.AllowedTopics)
	ignoredTopicsExpr, _ := compileRegexes(cfg.Topics.IgnoredTopics)

	service := &Service{
		Cfg:    cfg,
		logger: logger.Named("minion_service"),

		requestGroup: &singleflight.Group{},
		cache:        make(map[string]interface{}),
		cacheLock:    sync.RWMutex{},

		AllowedGroupIDsExpr: allowedGroupIDsExpr,
		IgnoredGroupIDsExpr: ignoredGroupIDsExpr,
		AllowedTopicsExpr:   allowedTopicsExpr,
		IgnoredTopicsExpr:   ignoredTopicsExpr,

		client:    client,
		admClient: kadm.NewClient(client),

		storage: storage,
	}

	return service, nil
}

func (s *Service) Start(ctx context.Context) error {
	err := s.ensureCompatibility(ctx)
	if err != nil {
		return fmt.Errorf("failed to check feature compatibility against Kafka: %w", err)
	}

	if s.Cfg.ConsumerGroups.Enabled && s.Cfg.ConsumerGroups.ScrapeMode == ConsumerGroupScrapeModeOffsetsTopic {
		go s.startConsumingOffsets(ctx)
	}

	return nil
}

func (s *Service) isReady() bool {
	if s.Cfg.ConsumerGroups.ScrapeMode == ConsumerGroupScrapeModeAdminAPI {
		return true
	}

	return s.storage.isReady()
}

func (s *Service) HandleIsReady() http.HandlerFunc {
	type response struct {
		StatusCode int `json:"statusCode"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		status := http.StatusOK
		if !s.isReady() {
			status = http.StatusServiceUnavailable
		}
		res := response{StatusCode: status}
		resJson, _ := json.Marshal(res)
		w.WriteHeader(status)
		w.Write(resJson)
	}
}

// ensureCompatibility checks whether the options as configured are available in the connected cluster. For example
// we will check if the target Kafka's API version support the LogDirs request. If that's not the case we will
// disable the option and print a warning message.
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
