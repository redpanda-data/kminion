package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

// Cluster is a module which connects to a Kafka Cluster and periodically fetches all topic and
// partition information (e. g. HighWaterMark). This information is passed to the storage module so that it can be used
// for consumer lag calculations.
type Cluster struct {
	// partitionWaterMarksCh is used to persist partition watermarks in memory so that they can be exposed with prometheus
	partitionWaterMarksCh chan *PartitionWaterMarks
	client                sarama.Client
	logger                *log.Entry
}

// PartitionWaterMarks contains the earliest and last known commited offset (highWaterMark) for a partition
type PartitionWaterMarks struct {
	TopicName     string
	PartitionID   int32
	HighWaterMark int64
	LowWaterMark  int64
	Timestamp     int64
}

// NewCluster creates a new cluster module and tries to connect to the kafka cluster
// If it cannot connect to the cluster it will panic
func NewCluster(opts *options.Options, partitionWaterMarksCh chan *PartitionWaterMarks) *Cluster {
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
	connectionLogger.Info("successfully connected to kafka cluster")

	return &Cluster{
		partitionWaterMarksCh: partitionWaterMarksCh,
		client:                client,
		logger:                logger,
	}
}

// Start starts cluster module
func (module *Cluster) Start() {
	// Initially trigger offset refresh once manually to ensure up to date data before the first ticker fires
	module.refreshAndSendTopicMetadata()

	offsetRefresh := time.NewTicker(time.Second * 5)
	go module.mainLoop(offsetRefresh)
}

func (module *Cluster) mainLoop(offsetRefresh *time.Ticker) {
	for {
		select {
		case <-offsetRefresh.C:
			module.refreshAndSendTopicMetadata()
		}
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
	requests, brokers := module.generateOffsetRequests(partitionIDsByTopicName)
	for brokerID, request := range requests {
		wg.Add(1)
		logger := module.logger.WithFields(log.Fields{
			"broker_id": brokerID,
		})
		go brokerOffsets(&wg, brokers[brokerID], request, logger, module.partitionWaterMarksCh)
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

func (module *Cluster) generateOffsetRequests(partitionIDsByTopicName map[string][]int32) (map[int32]*sarama.OffsetRequest, map[int32]*sarama.Broker) {
	requests := make(map[int32]*sarama.OffsetRequest)
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
			if _, exists := requests[broker.ID()]; !exists {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, partitionID, sarama.OffsetNewest, 1)
		}
	}

	return requests, brokers
}

func brokerOffsets(wg *sync.WaitGroup, broker *sarama.Broker, request *sarama.OffsetRequest, logger *log.Entry, ch chan<- *PartitionWaterMarks) {
	defer wg.Done()
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		logger.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("failed to fetch offsets from broker")
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
				}).Warn("error in OffsetResponse")
				continue
			}

			logger.WithFields(log.Fields{
				"topic":     topicName,
				"partition": partitionID,
				"offset":    offsetResponse.Offsets[0],
				"timestamp": ts,
			}).Debug("Got topic offset")
			entry := &PartitionWaterMarks{
				TopicName:     topicName,
				PartitionID:   partitionID,
				HighWaterMark: offsetResponse.Offsets[0],
				Timestamp:     ts,
			}
			ch <- entry
		}
	}
}
