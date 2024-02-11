//go:build integration

package integrationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	assertpkg "github.com/stretchr/testify/assert"
	requirepkg "github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"

	"github.com/cloudhut/kminion/v2/integrationtest/testcontainers/redpanda"
	"github.com/cloudhut/kminion/v2/integrationtest/testcontainers/toxiproxy"
)

type testLogConsumer struct{}

// Accept prints the log to stdout
func (lc testLogConsumer) Accept(l testcontainers.Log) {
	fmt.Print("CONTAINER: " + string(l.Content))
}

const (
	rpcRedpanda0ProxyPort = 11000
	rpcRedpanda1ProxyPort = 11001
	rpcRedpanda2ProxyPort = 11002
)

type Suite struct {
	suite.Suite

	// Containers
	redpanda0 *redpanda.Container
	redpanda1 *redpanda.Container
	redpanda2 *redpanda.Container

	// Toxiproxy
	toxiproxyCl         *toxiclient.Client
	proxyRpcRedpanda0   *toxiclient.Proxy
	proxyRpcRedpanda1   *toxiclient.Proxy
	proxyRpcRedpanda2   *toxiclient.Proxy
	proxyKafkaRedpanda0 *toxiclient.Proxy
	proxyKafkaRedpanda1 *toxiclient.Proxy
	proxyKafkaRedpanda2 *toxiclient.Proxy

	// Kafka Seeds
	kafkaSeedsWithProxy    []string
	kafkaSeedsWithoutProxy []string

	// Kafka Clients
	kafkaClWithProxy       *kgo.Client
	kafkaClWithoutProxy    *kgo.Client
	kafkaAdmClWithProxy    *kadm.Client
	kafkaAdmClWithoutProxy *kadm.Client
}

func TestSuite(t *testing.T) {
	suite.Run(t, &Suite{})
}

func (s *Suite) SetupSuite() {
	t := s.T()
	require := requirepkg.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. Setup shared docker network
	ntw, err := network.New(ctx)
	require.NoError(err)

	// 2. Start Toxiproxy container
	toxiproxyContainer, err := toxiproxy.RunContainer(ctx,
		network.WithNetwork([]string{"toxiproxy"}, ntw),
		testcontainers.WithLogConsumers(testLogConsumer{}),
		// Reserve three ports that can be exposed so that our local Go applications
		// can reach them. The actual address that will be used by the client and
		// in the advertised listener has to be discovered after the toxiproxy container
		// is started.
		toxiproxy.WithExposedPorts("12000/tcp", "12001/tcp", "12002/tcp"),
	)
	require.NoError(err)

	// Retrieve the host and port addresses that are mapped in the Docker host.
	// These shall be used by KMinion which is not running inside the same Docker
	// network and therefore also must be exposed as advertised listener.
	mappedKafkaHostRp0, mappedKafkaPortRp0, err := toxiproxyContainer.MappedHostPort(ctx, "12000/tcp")
	require.NoError(err)
	mappedKafkaHostRp1, mappedKafkaPortRp1, err := toxiproxyContainer.MappedHostPort(ctx, "12001/tcp")
	require.NoError(err)
	mappedKafkaHostRp2, mappedKafkaPortRp2, err := toxiproxyContainer.MappedHostPort(ctx, "12002/tcp")
	require.NoError(err)
	s.kafkaSeedsWithProxy = []string{
		fmt.Sprintf("%v:%v", mappedKafkaHostRp0, mappedKafkaPortRp0),
		fmt.Sprintf("%v:%v", mappedKafkaHostRp1, mappedKafkaPortRp1),
		fmt.Sprintf("%v:%v", mappedKafkaHostRp2, mappedKafkaPortRp2),
	}

	tpEndpoint, err := toxiproxyContainer.HTTPAddress(ctx)
	require.NoError(err)
	s.toxiproxyCl = toxiclient.NewClient(tpEndpoint)

	// Create proxies for all rpc listeners so that we can inject failure on the network
	// connection between all Redpanda nodes.
	rpcProxyRp0, err := s.toxiproxyCl.CreateProxy("rpc-redpanda-0", fmt.Sprintf("0.0.0.0:%v", rpcRedpanda0ProxyPort), "redpanda-0:30092")
	require.NoError(err)

	rpcProxyRp1, err := s.toxiproxyCl.CreateProxy("rpc-redpanda-1", fmt.Sprintf("0.0.0.0:%v", rpcRedpanda1ProxyPort), "redpanda-1:30092")
	require.NoError(err)

	rpcProxyRp2, err := s.toxiproxyCl.CreateProxy("rpc-redpanda-2", fmt.Sprintf("0.0.0.0:%v", rpcRedpanda2ProxyPort), "redpanda-2:30092")
	require.NoError(err)

	kafkaProxyRp0, err := s.toxiproxyCl.CreateProxy("kafka-redpanda-0", "0.0.0.0:12000", "redpanda-0:9095")
	require.NoError(err)
	kafkaProxyRp1, err := s.toxiproxyCl.CreateProxy("kafka-redpanda-1", "0.0.0.0:12001", "redpanda-1:9095")
	require.NoError(err)
	kafkaProxyRp2, err := s.toxiproxyCl.CreateProxy("kafka-redpanda-2", "0.0.0.0:12002", "redpanda-2:9095")
	require.NoError(err)

	s.proxyRpcRedpanda0 = rpcProxyRp0
	s.proxyRpcRedpanda1 = rpcProxyRp1
	s.proxyRpcRedpanda2 = rpcProxyRp2
	s.proxyKafkaRedpanda0 = kafkaProxyRp0
	s.proxyKafkaRedpanda1 = kafkaProxyRp1
	s.proxyKafkaRedpanda2 = kafkaProxyRp2

	// 3. Setup three Redpanda nodes
	seedServers := []redpanda.SeedServer{
		{
			Address: "toxiproxy",
			Port:    rpcRedpanda0ProxyPort,
		},
	}
	dockerImg := "redpandadata/redpanda:v23.3.5"

	// Start redpanda-0
	redpanda0, err := redpanda.RunContainer(ctx,
		testcontainers.WithImage(dockerImg),
		network.WithNetwork([]string{"redpanda-0"}, ntw),
		redpanda.WithKafkaListener(redpanda.KafkaListener{
			Name:                 "toxiproxy",
			Address:              "0.0.0.0",
			Port:                 9095,
			AdvertisedAddress:    mappedKafkaHostRp0,
			AdvertisedPort:       mappedKafkaPortRp0,
			AuthenticationMethod: "none",
		}),
		redpanda.WithRPCListener(redpanda.RPCListener{
			AdvertisedHost: "toxiproxy",
			AdvertisedPort: rpcRedpanda0ProxyPort,
		}),
		redpanda.WithSeedServers(seedServers...),
		testcontainers.WithLogConsumers(testLogConsumer{}),
	)
	require.NoError(err)
	s.redpanda0 = redpanda0

	// Start redpanda-1
	redpanda1, err := redpanda.RunContainer(ctx,
		testcontainers.WithImage(dockerImg),
		network.WithNetwork([]string{"redpanda-1"}, ntw),
		redpanda.WithKafkaListener(redpanda.KafkaListener{
			Name:                 "toxiproxy",
			Address:              "0.0.0.0",
			Port:                 9095,
			AdvertisedAddress:    mappedKafkaHostRp1,
			AdvertisedPort:       mappedKafkaPortRp1,
			AuthenticationMethod: "none",
		}),
		redpanda.WithRPCListener(redpanda.RPCListener{
			AdvertisedHost: "toxiproxy",
			AdvertisedPort: rpcRedpanda1ProxyPort,
		}),
		redpanda.WithSeedServers(seedServers...),
		testcontainers.WithLogConsumers(testLogConsumer{}),
	)
	require.NoError(err)
	s.redpanda1 = redpanda1

	// Start redpanda-2
	redpanda2, err := redpanda.RunContainer(ctx,
		testcontainers.WithImage(dockerImg),
		network.WithNetwork([]string{"redpanda-2"}, ntw),
		redpanda.WithKafkaListener(redpanda.KafkaListener{
			Name:                 "toxiproxy",
			Address:              "0.0.0.0",
			Port:                 9095,
			AdvertisedAddress:    mappedKafkaHostRp2,
			AdvertisedPort:       mappedKafkaPortRp2,
			AuthenticationMethod: "none",
		}),
		redpanda.WithRPCListener(redpanda.RPCListener{
			AdvertisedHost: "toxiproxy",
			AdvertisedPort: rpcRedpanda2ProxyPort,
		}),
		redpanda.WithSeedServers(seedServers...),
		testcontainers.WithLogConsumers(testLogConsumer{}),
	)
	require.NoError(err)
	s.redpanda2 = redpanda2

	rp0Seed, err := redpanda0.KafkaSeedBroker(ctx)
	require.NoError(err)
	rp1Seed, err := redpanda1.KafkaSeedBroker(ctx)
	require.NoError(err)
	rp2Seed, err := redpanda2.KafkaSeedBroker(ctx)
	require.NoError(err)
	s.kafkaSeedsWithoutProxy = []string{rp0Seed, rp1Seed, rp2Seed}

	// 3. Create Kafka clients
	kafkaClWithProxy, err := kgo.NewClient(kgo.SeedBrokers(s.kafkaSeedsWithProxy...))
	require.NoError(err)
	s.kafkaClWithProxy = kafkaClWithProxy
	s.kafkaAdmClWithProxy = kadm.NewClient(kafkaClWithProxy)

	kafkaClWithoutProxy, err := kgo.NewClient(kgo.SeedBrokers(s.kafkaSeedsWithoutProxy...))
	require.NoError(err)
	s.kafkaClWithoutProxy = kafkaClWithoutProxy
	s.kafkaAdmClWithoutProxy = kadm.NewClient(kafkaClWithoutProxy)

}

func (s *Suite) TearDownSuite() {
	t := s.T()
	assert := assertpkg.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	assert.NoError(s.redpanda0.Terminate(ctx))
}

func (s *Suite) TestListBrokers() {
	t := s.T()

	require := requirepkg.New(t)
	assert := assertpkg.New(t)

	t.Run("list brokers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()

		brokers, err := s.kafkaAdmClWithProxy.ListBrokers(ctx)
		require.NoError(err)
		assert.Len(brokers, 3)
	})
}
