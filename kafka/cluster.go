package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

// Cluster is a module which connects to a Kafka Cluster and periodically fetches all topic and
// partition information. This information is passed to the storage module so that it can be used
// for consumer lag calculations.
type Cluster struct {
	// StorageChannel is used to persist broker offsets in memory so that they can be exposed with prometheus
	storageChannel chan *OffsetEntry
	client         sarama.ClusterAdmin
	logger         *log.Entry
}

// NewCluster creates a new cluster module and tries to connect to the kafka cluster
// If it cannot connect to the cluster it will panic
func NewCluster(opts *options.Options, storageChannel chan *OffsetEntry) *Cluster {
	logger := log.WithFields(log.Fields{
		"module": "cluster",
	})

	// Connect client to at least one of the brokers and verify the connection by requesting metadata
	connectionLogger := logger.WithFields(log.Fields{
		"address": strings.Join(opts.KafkaBrokers, ","),
	})
	clientConfig := saramaClientConfig(opts)
	connectionLogger.Info("connecting to kafka cluster")
	client, err := sarama.NewClusterAdmin(opts.KafkaBrokers, clientConfig)
	if err != nil {
		connectionLogger.WithFields(log.Fields{
			"reason": err,
		}).Panicf("failed to start client")
	}
	connectionLogger.Info("successfully connected to kafka cluster")

	return &Cluster{
		storageChannel: storageChannel,
		client:         client,
		logger:         logger,
	}
}

// Start starts cluster module
func (module *Cluster) Start() {
	module.logger.Info("starting")

	offsetRefresh := time.NewTicker(time.Second * 5)
	topicRefresh := time.NewTicker(time.Second * 60)
	go module.mainLoop(offsetRefresh, topicRefresh)
}

func (module *Cluster) mainLoop(offsetRefresh *time.Ticker, topicRefresh *time.Ticker) {
	for {
		select {
		case <-offsetRefresh.C:
		case <-topicRefresh.C:
		}
	}
}

func (module *Cluster) getOffsets() {

}
