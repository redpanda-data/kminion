package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudhut/kminion/v2/kafka"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type Service struct {
	// General
	config Config
	logger *zap.Logger

	kafkaSvc *kafka.Service // creates kafka client for us
	client   *kgo.Client

	// Service
	minionID       string          // unique identifier, reported in metrics, in case multiple instances run at the same time
	groupId        string          // our own consumer group
	groupTracker   *groupTracker   // tracks consumer groups starting with the kminion prefix and deletes them if they are unused for some time
	messageTracker *messageTracker // tracks successfully produced messages,
	clientHooks    *clientHooks    // logs broker events, tracks the coordinator (i.e. which broker last responded to our offset commit)
	partitionCount int             // number of partitions of our test topic, used to send messages to all partitions

	// Metrics
	endToEndMessagesProduced prometheus.Counter
	endToEndMessagesAcked    prometheus.Counter
	endToEndMessagesReceived prometheus.Counter
	endToEndCommits          prometheus.Counter

	endToEndAckLatency       *prometheus.HistogramVec
	endToEndRoundtripLatency *prometheus.HistogramVec
	endToEndCommitLatency    *prometheus.HistogramVec
}

// NewService creates a new instance of the e2e moinitoring service (wow)
func NewService(cfg Config, logger *zap.Logger, kafkaSvc *kafka.Service, metricNamespace string, ctx context.Context) (*Service, error) {
	minionID := uuid.NewString()
	groupID := fmt.Sprintf("%v-%v", cfg.Consumer.GroupIdPrefix, minionID)

	// Producer options
	var kgoOpts []kgo.Opt
	if cfg.Producer.RequiredAcks == "all" {
		kgoOpts = append(kgoOpts, kgo.RequiredAcks(kgo.AllISRAcks()))
	} else {
		kgoOpts = append(kgoOpts, kgo.RequiredAcks(kgo.LeaderAck()))
		kgoOpts = append(kgoOpts, kgo.DisableIdempotentWrite())
	}
	kgoOpts = append(kgoOpts, kgo.ProduceRequestTimeout(cfg.Producer.AckSla))

	// Consumer configs
	kgoOpts = append(kgoOpts,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(cfg.TopicManagement.Name),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit())

	// Prepare hooks
	hooks := newEndToEndClientHooks(logger)
	kgoOpts = append(kgoOpts, kgo.WithHooks(hooks))

	// We use the manual partitioner so that the records' partition id will be used as target partition
	kgoOpts = append(kgoOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	// Create kafka service and check if client can successfully connect to Kafka cluster
	client, err := kafkaSvc.CreateAndTestClient(ctx, logger, kgoOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client for e2e: %w", err)
	}

	svc := &Service{
		config:   cfg,
		logger:   logger.Named("e2e"),
		kafkaSvc: kafkaSvc,
		client:   client,

		minionID:    minionID,
		groupId:     groupID,
		clientHooks: hooks,
	}

	svc.groupTracker = newGroupTracker(svc, ctx)
	svc.messageTracker = newMessageTracker(svc)

	makeCounter := func(name string, help string) prometheus.Counter {
		return promauto.NewCounter(prometheus.CounterOpts{
			Namespace: metricNamespace,
			Subsystem: "end_to_end",
			Name:      name,
			Help:      help,
		})
	}
	makeHistogramVec := func(name string, maxLatency time.Duration, labelNames []string, help string) *prometheus.HistogramVec {
		return promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Subsystem: "end_to_end",
			Name:      name,
			Help:      help,
			Buckets:   createHistogramBuckets(maxLatency),
		}, labelNames)
	}

	// Low-level info
	// Users can construct alerts like "can't produce messages" themselves from those
	svc.endToEndMessagesProduced = makeCounter("messages_produced_total", "Number of messages that kminion's end-to-end test has tried to send to kafka")
	svc.endToEndMessagesAcked = makeCounter("messages_acked_total", "Number of messages kafka acknowledged as produced")
	svc.endToEndMessagesReceived = makeCounter("messages_received_total", "Number of *matching* messages kminion received. Every roundtrip message has a minionID (randomly generated on startup) and a timestamp. Kminion only considers a message a match if it it arrives within the configured roundtrip SLA (and it matches the minionID)")
	svc.endToEndCommits = makeCounter("commits_total", "Counts how many times kminions end-to-end test has committed messages")

	// Latency Histograms
	// More detailed info about how long stuff took
	// Since histograms also have an 'infinite' bucket, they can be used to detect small hickups "lost" messages
	svc.endToEndAckLatency = makeHistogramVec("produce_latency_seconds", cfg.Producer.AckSla, []string{"partitionId"}, "Time until we received an ack for a produced message")
	svc.endToEndRoundtripLatency = makeHistogramVec("roundtrip_latency_seconds", cfg.Consumer.RoundtripSla, []string{"partitionId"}, "Time it took between sending (producing) and receiving (consuming) a message")
	svc.endToEndCommitLatency = makeHistogramVec("commit_latency_seconds", cfg.Consumer.CommitSla, []string{"groupCoordinatorBrokerId"}, "Time kafka took to respond to kminion's offset commit")

	return svc, nil
}

// Start starts the service (wow)
func (s *Service) Start(ctx context.Context) error {

	// Ensure topic exists and is configured correctly
	if err := s.validateManagementTopic(ctx); err != nil {
		return fmt.Errorf("could not validate end-to-end topic: %w", err)
	}

	// Get up-to-date metadata and inform our custom partitioner about the partition count
	topicMetadata, err := s.getTopicMetadata(ctx)
	if err != nil {
		return fmt.Errorf("could not get topic metadata after validation: %w", err)
	}
	partitions := len(topicMetadata.Topics[0].Partitions)
	s.partitionCount = partitions

	// finally start everything else (producing, consuming, continous validation, consumer group tracking)
	go s.initEndToEnd(ctx)

	return nil
}

func (s *Service) initEndToEnd(ctx context.Context) {

	validateTopicTicker := time.NewTicker(s.config.TopicManagement.ReconciliationInterval)
	produceTicker := time.NewTicker(s.config.ProbeInterval)
	commitTicker := time.NewTicker(5 * time.Second)
	// stop tickers when context is cancelled
	go func() {
		<-ctx.Done()
		produceTicker.Stop()
		validateTopicTicker.Stop()
		commitTicker.Stop()
	}()

	// keep checking end-to-end topic
	go func() {
		for range validateTopicTicker.C {
			err := s.validateManagementTopic(ctx)
			if err != nil {
				s.logger.Error("failed to validate end-to-end topic", zap.Error(err))
			}
		}
	}()

	// keep track of groups, delete old unused groups
	go s.groupTracker.start()

	// start consuming topic
	go s.startConsumeMessages(ctx)

	// start comitting offsets
	go func() {
		for range commitTicker.C {
			s.commitOffsets(ctx)
		}
	}()

	// start producing to topic
	go func() {
		for range produceTicker.C {
			s.produceLatencyMessages(ctx)
		}
	}()

}

// called from e2e when a message is acknowledged
func (s *Service) onAck(partitionId int32, duration time.Duration) {
	s.endToEndMessagesAcked.Inc()
	s.endToEndAckLatency.WithLabelValues(fmt.Sprintf("%v", partitionId)).Observe(duration.Seconds())
}

// called from e2e when a message completes a roundtrip (send to kafka, receive msg from kafka again)
func (s *Service) onRoundtrip(partitionId int32, duration time.Duration) {
	if duration > s.config.Consumer.RoundtripSla {
		return // message is too old
	}

	s.endToEndMessagesReceived.Inc()
	s.endToEndRoundtripLatency.WithLabelValues(fmt.Sprintf("%v", partitionId)).Observe(duration.Seconds())
}

// called from e2e when an offset commit is confirmed
func (s *Service) onOffsetCommit(brokerId int32, duration time.Duration) {

	// todo:
	// if the commit took too long, don't count it in 'commits' but add it to the histogram?
	// and how do we want to handle cases where we get an error??
	// should we have another metric that tells us about failed commits? or a label on the counter?
	brokerIdStr := fmt.Sprintf("%v", brokerId)
	s.endToEndCommitLatency.WithLabelValues(brokerIdStr).Observe(duration.Seconds())

	if duration > s.config.Consumer.CommitSla {
		return
	}

	s.endToEndCommits.Inc()
}
