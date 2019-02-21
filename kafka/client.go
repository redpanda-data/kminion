package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// Client offers all functionality to query the relevant kafka metrics
type Client struct {
	saramaConfig *sarama.Config
	client       sarama.Client
	consumer     sarama.Consumer
	brokersByID  map[int32]*sarama.Broker
}

// NewKafkaClient creates a new client to query all relevant data from Kafka
func NewKafkaClient(opts *options.Options) (*Client, error) {
	// try to start a kafka client
	log.Debug("Trying to create a sarama client config")
	clientConfig := sarama.NewConfig()
	clientConfig.ClientID = "kafka-lag-collector-1"
	clientConfig.Version = sarama.V0_11_0_2

	// SASL
	if opts.SASLEnabled {
		clientConfig.Net.SASL.Enable = true
		clientConfig.Net.SASL.Handshake = opts.UseSASLHandshake

		if opts.SASLUsername != "" {
			clientConfig.Net.SASL.User = opts.SASLUsername
		}
		if opts.SASLPassword != "" {
			clientConfig.Net.SASL.Password = opts.SASLPassword
		}
	}

	// TLS
	if opts.TLSEnabled {
		// Ensure that Cert and Key can be read
		canReadCertAndKey, err := CanReadCertAndKey(opts.TLSCertFilePath, opts.TLSKeyFilePath)
		if err != nil {
			log.Fatalln(err)
		}

		clientConfig.Net.TLS.Enable = true
		clientConfig.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.TLSInsecureSkipTLSVerify,
		}

		// Load CA file
		if opts.TLSCAFilePath != "" {
			if ca, err := ioutil.ReadFile(opts.TLSCAFilePath); err == nil {
				clientConfig.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				log.Fatalln(err)
			}
		}

		if canReadCertAndKey {
			cert, err := getCert(opts)
			if err == nil {
				clientConfig.Net.TLS.Config.Certificates = cert
			} else {
				log.Fatalln(err)
			}
		}
	}

	err := clientConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("Error validating kafka client config. %s", err)
	}
	log.Debug("Sarama client config has been created successfully")

	// Create sarama client
	log.Debug("Trying to create a new sarama client")
	sclient, err := sarama.NewClient(opts.KafkaBrokers, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("Error creating new sarama client. %s", err)
	}
	log.Debug("Sarama client has been successfully created")

	// Create sarama consumer
	consumer, err := sarama.NewConsumerFromClient(sclient)
	if err != nil {
		sclient.Close()
		return nil, fmt.Errorf("Error creating new consumer. %s", err)
	}

	return &Client{saramaConfig: clientConfig, client: sclient, consumer: consumer, brokersByID: make(map[int32]*sarama.Broker)}, nil
}

// getCert returns a Certificate from the CertFile and KeyFile in 'options',
// if the key is encrypted, the Passphrase in 'options' will be used to decrypt it.
func getCert(options *options.Options) ([]tls.Certificate, error) {
	if options.TLSCertFilePath == "" && options.TLSKeyFilePath == "" {
		return nil, fmt.Errorf("No file path specified for TLS key and certificate in environment variables")
	}

	errMessage := "Could not load X509 key pair. "

	cert, err := ioutil.ReadFile(options.TLSCertFilePath)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	prKeyBytes, err := ioutil.ReadFile(options.TLSKeyFilePath)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	prKeyBytes, err = getPrivateKey(prKeyBytes, options.TLSPassphrase)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	tlsCert, err := tls.X509KeyPair(cert, prKeyBytes)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return []tls.Certificate{tlsCert}, nil
}

// getPrivateKey returns the private key in 'keyBytes', in PEM-encoded format.
// If the private key is encrypted, 'passphrase' is used to decrypted the
// private key.
func getPrivateKey(keyBytes []byte, passphrase string) ([]byte, error) {
	// this section makes some small changes to code from notary/tuf/utils/x509.go
	pemBlock, _ := pem.Decode(keyBytes)
	if pemBlock == nil {
		return nil, fmt.Errorf("no valid private key found")
	}

	var err error
	if x509.IsEncryptedPEMBlock(pemBlock) {
		keyBytes, err = x509.DecryptPEMBlock(pemBlock, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("private key is encrypted, but could not decrypt it: '%s'", err)
		}
		keyBytes = pem.EncodeToMemory(&pem.Block{Type: pemBlock.Type, Bytes: keyBytes})
	}

	return keyBytes, nil
}

// GetTopicNames returns an array of topic names
func (c *Client) GetTopicNames() ([]string, error) {
	err := c.client.RefreshMetadata()
	if err != nil {
		return nil, fmt.Errorf("Cannot refresh metadata. %s", err)
	}

	topicNames, err := c.consumer.Topics()
	if err != nil {
		return nil, fmt.Errorf("Cannot fetch topic names. %s", err)
	}

	return topicNames, nil
}

// IsHealthy returns true if communication with kafka brokers is fine
func (c *Client) IsHealthy() bool {
	err := c.client.RefreshMetadata()
	if err != nil {
		return false
	}

	return true
}

// GetPartitionIDs returns an int32 array with all partitionIDs for a specific topic
func (c *Client) GetPartitionIDs(topicName string) ([]int32, error) {
	partitionIDs, err := c.client.Partitions(topicName)
	if err != nil {
		return nil, fmt.Errorf("Cannot get partitions. %s", err)
	}

	return partitionIDs, nil
}

// GetPartitionIDsBulk returns a map of partitionIDs for each topic. The topic name is the map's key.
// Removes a topic completely if it could not get one or more partitions
func (c *Client) GetPartitionIDsBulk(topicNames []string) map[string][]int32 {
	type partitionIDResponse struct {
		PartitionIDs []int32
		TopicName    string
	}
	ch := make(chan partitionIDResponse, 50)
	var wg sync.WaitGroup
	for _, topicName := range topicNames {
		if !isValidTopicName(topicName) {
			continue
		}

		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			partitionIDs, err := c.client.Partitions(topic)
			if err != nil {
				log.Warnf("Cannot get partitions for topic '%v'. %s", topic, err)
				ch <- partitionIDResponse{}
				return
			}
			ch <- partitionIDResponse{TopicName: topic, PartitionIDs: partitionIDs}
		}(topicName)
	}

	// Close channel when workers are done
	go func() {
		wg.Wait()
		close(ch)
	}()

	response := make(map[string][]int32)
	for partitionIDResponse := range ch {
		topicName := partitionIDResponse.TopicName
		response[topicName] = partitionIDResponse.PartitionIDs
	}

	return response
}

// ConsumerGroupTopicLags fetches metrics for all existing consumer groups of a given kafka broker. Therefore it does:
// 1. Fetch a list of all consumer groups
// 2. Describe each group from 1
// 3. Create an offsetFetchRequest for each group
func (c *Client) ConsumerGroupTopicLags(topicsByName map[string]*Topic) map[string][]*ConsumerGroupTopicLag {
	type consumerGroupOffset struct {
		GroupName      string
		offsetResponse *sarama.OffsetFetchResponse
	}
	offsetFetchResponseCh := make(chan *consumerGroupOffset, 100)

	// for each broker get all consumer groups
	var wg sync.WaitGroup
	for _, broker := range c.brokersByID {
		wg.Add(1)

		// Start a worker routine for each broker
		go func(b *sarama.Broker, ch chan<- *consumerGroupOffset) {
			defer wg.Done()
			err := b.Open(c.saramaConfig)
			if err != nil && err != sarama.ErrAlreadyConnected {
				log.Warnf("Cannot connect to broker '%v': %s", b.ID(), err)
				return
			}
			defer b.Close()

			// Get a list of all consumer groups for this broker
			groups, err := b.ListGroups(&sarama.ListGroupsRequest{})
			if err != nil {
				log.Warnf("Cannot list consumer groups of broker '%v': %s", b.ID(), err)
				return
			}

			var groupIDs []string
			for groupID := range groups.Groups {
				groupIDs = append(groupIDs, groupID)
			}

			// Describe all groups which we just got a list from
			describeGroups, err := b.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIDs})
			if err != nil {
				log.Warnf("Cannot describe groups of broker '%v': %s", b.ID(), err)
				return
			}

			// Fetch consumer group offsets from all groups
			for _, describedGroup := range describeGroups.Groups {
				offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: describedGroup.GroupId, Version: 1}

				for topicName, topic := range topicsByName {
					for _, partition := range topic.Partitions {
						offsetFetchRequest.AddPartition(topicName, partition.PartitionID)
					}
				}

				offsetFetchResponse, err := b.FetchOffset(&offsetFetchRequest)
				if err != nil {
					log.Warnf("Cannot fetch consumer group offsets of broker '%v', group '%s'", b.ID(), describedGroup.GroupId)
					continue
				}
				ch <- &consumerGroupOffset{GroupName: describedGroup.GroupId, offsetResponse: offsetFetchResponse}
			}
		}(broker, offsetFetchResponseCh)
	}

	// Close channel when workers are done
	go func() {
		wg.Wait()
		close(offsetFetchResponseCh)
	}()

	consumerGroupLags := make(map[string][]*ConsumerGroupTopicLag)
	for groupOffset := range offsetFetchResponseCh {
		for topicName, offsetResponse := range groupOffset.offsetResponse.Blocks {
			// In case the topic is not being consumed by that consumer group, we will ignore that topic
			isTopicBeingConsumed := false
			for _, partitionOffset := range offsetResponse {
				// -1 Offset means this consumergroup does not consume the given partition
				if partitionOffset.Offset != -1 {
					isTopicBeingConsumed = true
				}
			}
			if isTopicBeingConsumed == false {
				continue
			}

			var topicLag int64
			for partition, partitionOffset := range offsetResponse {
				if partitionOffset.Err != sarama.ErrNoError {
					log.Warnf("Cannot get partition offset for partition '%d', %v", partition, partitionOffset.Err)
					continue
				}
				consumerPartitionOffset := partitionOffset.Offset
				highWaterMark := topicsByName[topicName].HighWaterMarkByPartitionID(partition)
				partitionLag := highWaterMark - consumerPartitionOffset
				// partitionLag might be negative due to the delay between fetching the partitions' high watermarks and the consumer group's offsets
				if partitionLag < 0 {
					partitionLag = 0
				}
				topicLag = topicLag + partitionLag
				log.Debugf("Partition lag for group '%s': '%d'", groupOffset.GroupName, partitionLag)
			}
			log.Debugf("Consumergroup '%s' - Topic: '%s' - Lag: '%d'", groupOffset.GroupName, topicName, topicLag)
			if _, arrayExists := consumerGroupLags[groupOffset.GroupName]; !arrayExists {
				consumerGroupLags[groupOffset.GroupName] = make([]*ConsumerGroupTopicLag, 0)
			}
			groupLag := &ConsumerGroupTopicLag{Name: groupOffset.GroupName, TopicName: topicName, TopicLag: topicLag}
			consumerGroupLags[groupOffset.GroupName] = append(consumerGroupLags[groupOffset.GroupName], groupLag)
		}
	}
	return consumerGroupLags
}

// GetPartitionOffsets returns a map of Topics where the topic name is the key. Each topic contains a list of all it's
// partitions. Each partition has an oldest offset and highWaterMark (newest offset). If a topic is missing a single partition metric
// due to an error it won't be returned at all. Thus it is guaranteed that all reported topic metrics are valid.
func (c *Client) GetPartitionOffsets(partitionIDsByTopicName map[string][]int32) map[string]*Topic {
	// Sending these requests in bulk to the brokers is way faster and more performant than doing them one by one
	// We have to use two seperate offsetRequest buckets (one for oldest, one for newest offsets) because
	// we can not differ in the offset response what offset (newest or oldest) it is
	newestOffsetRequestsByBrokerID := make(map[int32]*sarama.OffsetRequest)
	oldestOffsetRequestsByBrokerID := make(map[int32]*sarama.OffsetRequest)
	errorTopics := make(map[string]bool)
	errorTopicsMutex := &sync.Mutex{}
	requestsByBrokerIDMutex := &sync.Mutex{}

	var wg sync.WaitGroup
	brokersByID := make(map[int32]*sarama.Broker)
	for topic, partitionIDs := range partitionIDsByTopicName {
		for _, partitionID := range partitionIDs {
			wg.Add(1)
			go func(topic string, partitionID int32) {
				defer wg.Done()
				broker, err := c.client.Leader(topic, partitionID)
				if err != nil {
					log.Warnf("Topic leader error on topic '%s', partition: '%v': %v", topic, partitionID, err)
					errorTopicsMutex.Lock()
					errorTopics[topic] = true
					errorTopicsMutex.Unlock()
					return
				}
				brokerID := broker.ID()

				// Create OffsetRequest if not yet existent in bucket
				requestsByBrokerIDMutex.Lock()
				if newestOffsetRequestsByBrokerID[brokerID] == nil {
					newestOffsetRequestsByBrokerID[brokerID] = &sarama.OffsetRequest{}
				}
				if oldestOffsetRequestsByBrokerID[brokerID] == nil {
					oldestOffsetRequestsByBrokerID[brokerID] = &sarama.OffsetRequest{}
				}
				brokersByID[brokerID] = broker
				newestOffsetRequestsByBrokerID[brokerID].AddBlock(topic, partitionID, sarama.OffsetNewest, 1)
				oldestOffsetRequestsByBrokerID[brokerID].AddBlock(topic, partitionID, sarama.OffsetOldest, 1)
				requestsByBrokerIDMutex.Unlock()
			}(topic, partitionID)
		}
	}
	wg.Wait()
	c.brokersByID = brokersByID

	// Send out the OffsetRequest to each broker for all partitions it is leader for
	offsetsCh := make(chan brokerOffsetResponse, 30)

	for brokerID, requestBlocks := range newestOffsetRequestsByBrokerID {
		broker := brokersByID[brokerID]
		wg.Add(1)
		go getOffsetsFromBroker(offsetsCh, &wg, requestBlocks, broker, newestOffset)
	}
	for brokerID, requestBlocks := range oldestOffsetRequestsByBrokerID {
		broker := brokersByID[brokerID]
		wg.Add(1)
		go getOffsetsFromBroker(offsetsCh, &wg, requestBlocks, broker, oldestOffset)
	}

	// Close channel when workers are done
	go func() {
		wg.Wait()
		close(offsetsCh)
	}()

	// Aggregate sum all partition offsets by topic
	topicsByName := make(map[string]*Topic)
	for brokerResponse := range offsetsCh {
		for _, val := range brokerResponse.partitionOffsets {
			// Create new topic for that topic name if necessray
			if _, ok := topicsByName[val.TopicName]; !ok {
				topicsByName[val.TopicName] = &Topic{Name: val.TopicName, Partitions: make(map[int32]*Partition)}
			}
			// Create new partition entry for that partitionID if necessary
			if _, ok := topicsByName[val.TopicName].Partitions[val.PartitionID]; !ok {
				topicsByName[val.TopicName].Partitions[val.PartitionID] = &Partition{PartitionID: val.PartitionID}
			}
			if val.OffsetType == oldestOffset {
				topicsByName[val.TopicName].Partitions[val.PartitionID].OldestOffset = val.Offset
			} else {
				topicsByName[val.TopicName].Partitions[val.PartitionID].HighWaterMark = val.Offset
			}
		}

		for _, topic := range brokerResponse.errorTopics {
			errorTopicsMutex.Lock()
			errorTopics[topic] = true
			errorTopicsMutex.Unlock()
		}
	}

	// Now that we've got oldest and newest offsets, we can calculate topic lag and message count by
	// iterating over the partitions again
	for _, topic := range topicsByName {
		// Delete topics which had any errors (e. g. a missing partition offset)
		_, isErrored := errorTopics[topic.Name]
		isMissingPartitions := len(partitionIDsByTopicName[topic.Name]) != len(topic.Partitions) // Needed if broker errors during offset response
		if isErrored || isMissingPartitions {
			delete(topicsByName, topic.Name)
			log.Warnf("Deleted topic '%v' because it either had an error or is missing partition metrics. %+v, %+v", topic.Name, isErrored, isMissingPartitions)
			continue
		}

		for _, partition := range topic.Partitions {
			topicsByName[topic.Name].MessageCount = partition.MessageCount()
		}
	}

	return topicsByName
}

func getOffsetsFromBroker(ch chan<- brokerOffsetResponse, wg *sync.WaitGroup, requestBlocks *sarama.OffsetRequest, broker *sarama.Broker, oType offsetType) {
	defer wg.Done()
	offsetsResponse, err := broker.GetAvailableOffsets(requestBlocks)
	if err != nil {
		broker.Close()
		log.Warnf("Cannot fetch offsets from broker %v: %v", broker.ID(), err)
		return
	}

	errorTopics := make([]string, 0)
	topicPartitionOffsets := make([]topicPartitionOffset, 0)
	for topic, partitions := range offsetsResponse.Blocks {
		for partitionID, offsetResponse := range partitions {
			if offsetResponse.Err != sarama.ErrNoError {
				errorTopics = append(errorTopics, topic)
				log.Warnf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partitionID, broker.ID(), offsetResponse.Err.Error())
				continue
			}

			offset := offsetResponse.Offsets[0]
			log.Debugf("Partition offset (oldest/newest) for topic '%v', partition '%d' is '%d'", topic, partitionID, offset)
			partitionOffset := topicPartitionOffset{TopicName: topic, PartitionID: partitionID, Offset: offset, OffsetType: oType}
			topicPartitionOffsets = append(topicPartitionOffsets, partitionOffset)
		}
	}
	ch <- brokerOffsetResponse{partitionOffsets: topicPartitionOffsets, errorTopics: errorTopics}
}

// isValidTopicName returns a bool which indicates whether metrics for this topic should be exposed or not
func isValidTopicName(topicName string) bool {
	if strings.HasPrefix(topicName, "__") || strings.HasPrefix(topicName, "_confluent") {
		return false
	}
	return true
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}
