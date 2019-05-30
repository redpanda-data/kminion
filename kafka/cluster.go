package kafka

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
)

// Cluster is a module which connects to a Kafka Cluster and periodically fetches all topic and
// partition information (e. g. High & Low water marks). This information is passed to the storage
// module where it can be retrieved by the prometheus collector to expose metrics.
type Cluster struct {
	// storageCh is used to persist partition watermarks in memory so that they can be exposed with prometheus
	storageCh   chan<- *StorageRequest
	client      sarama.Client
	admin       sarama.ClusterAdmin
	logger      *log.Entry
	options     *options.Options
	topicByName map[string]*sarama.TopicMetadata
}

// PartitionWaterMark contains either the first or last known committed offset (water mark) for a partition
type PartitionWaterMark struct {
	TopicName   string
	PartitionID int32
	WaterMark   int64
	Timestamp   int64
}

// TopicConfiguration indicates config entries for a topic along with the partition count
type TopicConfiguration struct {
	TopicName      string
	PartitionCount int
	CleanupPolicy  string
}

// consumerOffsetTopic holds PartitionHighWatermarks for the __consumer_offsets topic
type consumerOffsetTopic struct {
	Lock           sync.RWMutex
	PartitionsByID map[int32]consumerOffsetPartition
}

// consumerOffsetPartition represents the high water mark for a single partition of the __consumer_offsets topic
type consumerOffsetPartition struct {
	PartitionID   int32
	HighWaterMark int64
}

var (
	// offsetWaterMarks is used to determine if partition consumers have caught up the partition lag
	offsetWaterMarks = consumerOffsetTopic{
		PartitionsByID: make(map[int32]consumerOffsetPartition),
	}
)

// NewCluster creates a new cluster module and tries to connect to the kafka cluster
// If it cannot connect to the cluster it will panic
func NewCluster(opts *options.Options, storageCh chan<- *StorageRequest) *Cluster {
	logger := log.WithFields(log.Fields{
		"module": "cluster",
	})

	// Connect client to at least one of the brokers and verify the connection by requesting metadata
	connectionLogger := logger.WithFields(log.Fields{
		"address": strings.Join(opts.KafkaBrokers, ","),
	})

	clientConfig := saramaClientConfig(opts)
	connectionLogger.Info("connecting to kafka cluster")
	client, err := sarama.NewClient(opts.KafkaBrokers, clientConfig)
	if err != nil {
		connectionLogger.WithFields(log.Fields{
			"reason": err,
		}).Panicf("failed to start client")
	}

	admin, err := sarama.NewClusterAdmin(opts.KafkaBrokers, clientConfig)
	if err != nil {
		connectionLogger.WithFields(log.Fields{
			"reason": err,
		}).Panicf("failed to start admin client")
	}
	connectionLogger.Info("successfully connected to kafka cluster")

	return &Cluster{
		storageCh: storageCh,
		client:    client,
		admin:     admin,
		logger:    logger,
		options:   opts,
	}
}

// Start starts cluster module
func (module *Cluster) Start() {
	go module.mainLoop()
}

// IsHealthy returns true if there is at least one broker which can be talked to
func (module *Cluster) IsHealthy() bool {
	if len(module.client.Brokers()) > 0 {
		return true
	}

	return false
}

func (module *Cluster) mainLoop() {

	go func() {
		// Initially trigger offset refresh once manually to ensure up to date data before the first ticker fires
		module.refreshAndSendTopicMetadata()
		offsetRefresh := time.NewTicker(time.Second * 5)
		for range offsetRefresh.C {
			module.refreshAndSendTopicMetadata()
		}
	}()

	go func() {
		// Initially trigger offset refresh once manually to ensure up to date data before the first ticker fires
		module.refreshAndSendTopicConfig()
		topicConfigRefresh := time.NewTicker(time.Second * 60)
		for range topicConfigRefresh.C {
			module.refreshAndSendTopicConfig()
		}
	}()
}

// deleteTopicIfNeeded checks a current map of available topics against a previously fetched
// map and sends a delete topic request for topics which are not existent anymore.
func (module *Cluster) deleteTopicIfNeeded(topicByName map[string]*sarama.TopicMetadata) {
	if module.topicByName != nil {
		for topicName := range module.topicByName {
			if _, exists := topicByName[topicName]; !exists {
				// Topic previously existed, but now it doesn't exist anymore, send delete request
				module.logger.WithFields(log.Fields{
					"topic": topicName,
				}).Info("topic no longer exists, deleting it from storage")
				module.storageCh <- newDeleteTopicRequest(topicName)
			}
		}
	}

	module.topicByName = topicByName
}

func (module *Cluster) refreshAndSendTopicConfig() {
	broker := module.getAnyBroker()
	if broker == nil {
		module.logger.WithFields(log.Fields{
			"error": "no brokers available",
		}).Warn("failed to get active broker to refresh topic config")
		return
	}

	metadataReq := &sarama.MetadataRequest{}
	metadata, err := broker.GetMetadata(metadataReq)
	if err != nil {
		module.logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("failed to get metadata")
		return
	}

	// Prepare config request which contains all topic requests.
	// This way we can avoid sending many requests
	var describeConfigsResources []*sarama.ConfigResource
	topicByName := make(map[string]*sarama.TopicMetadata)
	for _, topic := range metadata.Topics {
		topicByName[topic.Name] = topic
		topicResource := &sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        topic.Name,
			ConfigNames: []string{"cleanup.policy"},
		}
		describeConfigsResources = append(describeConfigsResources, topicResource)
	}
	module.deleteTopicIfNeeded(topicByName)

	request := &sarama.DescribeConfigsRequest{
		Resources: describeConfigsResources,
	}
	response, err := broker.DescribeConfigs(request)
	if err != nil {
		module.logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("failed to get describe configs response")
		return
	}

	for _, resource := range response.Resources {
		if resource.ErrorCode != 0 || len(resource.Configs) == 0 {
			continue
		}

		partitionCount := len(topicByName[resource.Name].Partitions)
		config := &TopicConfiguration{
			TopicName:      resource.Name,
			PartitionCount: partitionCount,
			CleanupPolicy:  resource.Configs[0].Value,
		}
		module.storageCh <- newAddTopicConfig(config)
	}
}

// refreshAndSendTopicMetadata fetches topic offsets and partitionIDs for each topic:partition and
// sends this information to the storage module
func (module *Cluster) refreshAndSendTopicMetadata() {
	partitionIDsByTopicName, err := module.topicPartitions()
	if err != nil {
		return
	}

	// Send requests in bulk to each broker for those partitions it is responsible/leader for
	var wg = sync.WaitGroup{}
	module.logger.Debug("starting to collect topic offsets")
	highRequests, lowRequests, brokers := module.generateOffsetRequests(partitionIDsByTopicName)
	for brokerID, request := range highRequests {
		wg.Add(1)
		logger := module.logger.WithFields(log.Fields{
			"broker_id": brokerID,
		})
		go module.processHighWaterMarks(&wg, brokers[brokerID], request, logger)
	}
	wg.Wait() // Await offsets first, to prevent concurrent access on brokers
	for brokerID, request := range lowRequests {
		wg.Add(1)
		logger := module.logger.WithFields(log.Fields{
			"broker_id": brokerID,
		})
		go module.processLowWaterMarks(&wg, brokers[brokerID], request, logger)
	}
	wg.Wait()
	module.logger.Debug("collected topic offsets")
}

// topicPartitions returns a map of all partition IDs (value) grouped by topic name (key)
func (module *Cluster) topicPartitions() (map[string][]int32, error) {
	err := module.client.RefreshMetadata()
	if err != nil {
		module.logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("could not refresh topic metadata")
	}

	// Get the current list of topics and make a map
	topicNames, err := module.client.Topics()
	if err != nil {
		module.logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("failed to fetch topic list")
		return nil, fmt.Errorf("failed to fetch topic list")
	}

	partitionIDsByTopicName := make(map[string][]int32)
	for _, topicName := range topicNames {
		// Partitions() response is served from cached metadata if available. So there's usually no need to launch go routines for that
		partitionIDs, err := module.client.Partitions(topicName)
		if err != nil {
			module.logger.WithFields(log.Fields{
				"error": err.Error,
				"topic": topicName,
			}).Error("failed to fetch partition list")
		}

		partitionIDsByTopicName[topicName] = make([]int32, len(partitionIDs))
		for _, partitionID := range partitionIDs {
			partitionIDsByTopicName[topicName] = append(partitionIDsByTopicName[topicName], partitionID)
		}
	}

	return partitionIDsByTopicName, nil
}

func (module *Cluster) generateOffsetRequests(partitionIDsByTopicName map[string][]int32) (map[int32]*sarama.OffsetRequest, map[int32]*sarama.OffsetRequest, map[int32]*sarama.Broker) {
	// we must create two separate buckets for high & low watermarks, because adding a request block
	// with same topic:partition but different time will still result in just one request, see:
	// https://github.com/Shopify/sarama/blob/master/offset_request.go AddBlock() method
	highWaterMarkRequests := make(map[int32]*sarama.OffsetRequest)
	lowWaterMarkRequests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	// Generate an OffsetRequest for each topic:partition and bucket it to the leader broker
	for topic, partitionIDs := range partitionIDsByTopicName {
		for _, partitionID := range partitionIDs {
			broker, err := module.client.Leader(topic, partitionID)
			if err != nil {
				module.logger.WithFields(log.Fields{
					"topic":     topic,
					"partition": partitionID,
					"error":     err.Error(),
				}).Warn("failed to fetch leader for partition")
				continue
			}
			if _, exists := highWaterMarkRequests[broker.ID()]; !exists {
				highWaterMarkRequests[broker.ID()] = &sarama.OffsetRequest{}
				lowWaterMarkRequests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			highWaterMarkRequests[broker.ID()].AddBlock(topic, partitionID, sarama.OffsetNewest, 1)
			lowWaterMarkRequests[broker.ID()].AddBlock(topic, partitionID, sarama.OffsetOldest, 1)
		}
	}

	return highWaterMarkRequests, lowWaterMarkRequests, brokers
}

func (module *Cluster) processHighWaterMarks(wg *sync.WaitGroup, broker *sarama.Broker, request *sarama.OffsetRequest, logger *log.Entry) {
	defer wg.Done()
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("failed to fetch high watermarks from broker")
		broker.Close()
		return
	}
	ts := time.Now().Unix() * 1000
	for topicName, responseBlock := range response.Blocks {

		for partitionID, offsetResponse := range responseBlock {
			if offsetResponse.Err != sarama.ErrNoError {
				logger.WithFields(log.Fields{
					"error":     offsetResponse.Err.Error(),
					"topic":     topicName,
					"partition": partitionID,
				}).Warn("error in high OffsetResponse")
				continue
			}

			if topicName == module.options.ConsumerOffsetsTopicName {
				offsetWaterMarks.Lock.Lock()
				offsetWaterMarks.PartitionsByID[partitionID] = consumerOffsetPartition{
					PartitionID:   partitionID,
					HighWaterMark: offsetResponse.Offsets[0],
				}
				offsetWaterMarks.Lock.Unlock()
			}

			// Skip topic in this for loop (instead of the outer one) because we still need __consumer_offset information
			if !module.isTopicAllowed(topicName) {
				continue
			}
			logger.WithFields(log.Fields{
				"topic":     topicName,
				"partition": partitionID,
				"offset":    offsetResponse.Offsets[0],
				"timestamp": ts,
			}).Debug("received partition high water mark")
			entry := &PartitionWaterMark{
				TopicName:   topicName,
				PartitionID: partitionID,
				WaterMark:   offsetResponse.Offsets[0],
				Timestamp:   ts,
			}
			module.storageCh <- newAddPartitionHighWaterMarkRequest(entry)
		}
	}
}

func (module *Cluster) processLowWaterMarks(wg *sync.WaitGroup, broker *sarama.Broker, request *sarama.OffsetRequest, logger *log.Entry) {
	defer wg.Done()
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("failed to fetch low watermarks from broker")
		broker.Close()
		return
	}
	ts := time.Now().Unix() * 1000
	for topicName, responseBlock := range response.Blocks {
		if !module.isTopicAllowed(topicName) {
			continue
		}

		for partitionID, offsetResponse := range responseBlock {
			if offsetResponse.Err != sarama.ErrNoError {
				logger.WithFields(log.Fields{
					"error":     offsetResponse.Err.Error(),
					"topic":     topicName,
					"partition": partitionID,
				}).Warn("error in low OffsetResponse")
				continue
			}

			logger.WithFields(log.Fields{
				"topic":     topicName,
				"partition": partitionID,
				"offset":    offsetResponse.Offsets[0],
				"timestamp": ts,
			}).Debug("received partition low water mark")
			entry := &PartitionWaterMark{
				TopicName:   topicName,
				PartitionID: partitionID,
				WaterMark:   offsetResponse.Offsets[0],
				Timestamp:   ts,
			}
			module.storageCh <- newAddPartitionLowWaterMarkRequest(entry)
		}
	}
}

// getAnyBroker return a random item from the brokers slice
func (module *Cluster) getAnyBroker() *sarama.Broker {
	brokers := module.client.Brokers()

	connectedBrokers := make([]*sarama.Broker, 0)
	for _, broker := range brokers {
		connected, err := broker.Connected()
		if err == nil && connected == true {
			connectedBrokers = append(connectedBrokers, broker)
		}
	}
	length := len(connectedBrokers)
	if length == 0 {
		return nil
	}

	rand.Seed(time.Now().Unix())
	n := rand.Int() % len(connectedBrokers)

	return connectedBrokers[n]
}

func (module *Cluster) isTopicAllowed(topicName string) bool {
	if module.options.IgnoreSystemTopics {
		if strings.HasPrefix(topicName, "__") || strings.HasPrefix(topicName, "_confluent") {
			return false
		}
	}

	return true
}
