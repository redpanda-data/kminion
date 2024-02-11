package redpanda

import (
	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	// Superusers is a list of service account names.
	Superusers []string

	// KafkaEnableAuthorization is a flag to require authorization for Kafka connections.
	KafkaEnableAuthorization bool

	// KafkaAuthenticationMethod is either "none" for plaintext or "sasl"
	// for SASL (scram) authentication.
	KafkaAuthenticationMethod string

	// SchemaRegistryAuthenticationMethod is either "none" for no authentication
	// or "http_basic" for HTTP basic authentication.
	SchemaRegistryAuthenticationMethod string

	// EnableWasmTransform is a flag to enable wasm transform.
	EnableWasmTransform bool

	// ServiceAccounts is a map of username (key) to password (value) of users
	// that shall be created, so that you can use these to authenticate against
	// Redpanda (either for the Kafka API or Schema Registry HTTP access).
	// You must use SCRAM-SHA-256 as algorithm when authenticating on the
	// Kafka API.
	ServiceAccounts map[string]string

	// AutoCreateTopics is a flag to allow topic auto creation.
	AutoCreateTopics bool

	// EnableTLS is a flag to enable TLS.
	EnableTLS bool

	// NetworkAliases can be used to resolve the container by another name inside
	// the networks the container is attached to.
	NetworkAliases []string

	cert, key []byte

	// KafkaListeners is a list of custom Kafka listeners that can be provided
	// to access the containers form within docker networks
	KafkaListeners []KafkaListener

	// RedpandaRPCListener is the advertised RPC listener that is used for the
	// internal internode communication for all cluster nodes.
	RedpandaRPCListener *RPCListener

	// SeedServers is a list of the server addresses and ports used to
	// join current cluster.
	SeedServers []SeedServer
}

func defaultOptions() options {
	return options{
		Superusers:                         []string{},
		KafkaEnableAuthorization:           false,
		KafkaAuthenticationMethod:          "none",
		SchemaRegistryAuthenticationMethod: "none",
		ServiceAccounts:                    make(map[string]string, 0),
		AutoCreateTopics:                   false,
		EnableTLS:                          false,
		NetworkAliases:                     []string{},
		KafkaListeners:                     make([]KafkaListener, 0),
		RedpandaRPCListener:                nil,
		SeedServers:                        nil,
	}
}

// Compiler check to ensure that Option implements the testcontainers.ContainerCustomizer interface.
var _ testcontainers.ContainerCustomizer = (*Option)(nil)

// Option is an option for the Redpanda container.
type Option func(*options)

// Customize is a NOOP. It's defined to satisfy the testcontainers.ContainerCustomizer interface.
func (o Option) Customize(*testcontainers.GenericContainerRequest) {
	// NOOP to satisfy interface.
}

func WithNewServiceAccount(username, password string) Option {
	return func(o *options) {
		o.ServiceAccounts[username] = password
	}
}

// WithSuperusers defines the superusers added to the redpanda config.
// By default, there are no superusers.
func WithSuperusers(superusers ...string) Option {
	return func(o *options) {
		o.Superusers = superusers
	}
}

// WithEnableSASL enables SASL scram sha authentication.
// By default, no authentication (plaintext) is used.
// When setting an authentication method, make sure to add users
// as well as authorize them using the WithSuperusers() option.
func WithEnableSASL() Option {
	return func(o *options) {
		o.KafkaAuthenticationMethod = "sasl"
	}
}

// WithEnableKafkaAuthorization enables authorization for connections on the Kafka API.
func WithEnableKafkaAuthorization() Option {
	return func(o *options) {
		o.KafkaEnableAuthorization = true
	}
}

// WithEnableWasmTransform enables wasm transform.
// Should not be used with RP versions before 23.3
func WithEnableWasmTransform() Option {
	return func(o *options) {
		o.EnableWasmTransform = true
	}
}

// WithEnableSchemaRegistryHTTPBasicAuth enables HTTP basic authentication for
// Schema Registry.
func WithEnableSchemaRegistryHTTPBasicAuth() Option {
	return func(o *options) {
		o.SchemaRegistryAuthenticationMethod = "http_basic"
	}
}

// WithAutoCreateTopics enables topic auto creation.
func WithAutoCreateTopics() Option {
	return func(o *options) {
		o.AutoCreateTopics = true
	}
}

func WithTLS(cert, key []byte) Option {
	return func(o *options) {
		o.EnableTLS = true
		o.cert = cert
		o.key = key
	}
}

func WithNetworkAliases(aliases ...string) Option {
	return func(o *options) {
		o.NetworkAliases = append(o.NetworkAliases, aliases...)
	}
}

// WithKafkaListener adds a custom KafkaListener to the Redpanda containers. Listener
// will be aliases to all networks, so they can be accessed from within docker
// networks. At least one network must be attached to the container, if not an
// error will be thrown when starting the container.
func WithKafkaListener(l KafkaListener) Option {
	return func(o *options) {
		o.KafkaListeners = append(o.KafkaListeners, l)
	}
}

// WithRPCListener sets a custom rpc listener to the Redpanda container that is
// used for the internal RPC communication between this and other redpanda containers.
func WithRPCListener(l RPCListener) Option {
	return func(o *options) {
		o.RedpandaRPCListener = &l
	}
}

// WithSeedServers adds the provided servers to the seed_servers list. These
// are used to join current cluster.
func WithSeedServers(seeds ...SeedServer) Option {
	return func(o *options) {
		o.SeedServers = append(o.SeedServers, seeds...)
	}
}
