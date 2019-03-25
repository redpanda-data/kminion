package options

// Options are configuration options that can be set by Environment Variables
// Version - Application version
// Kafka Broker string
type Options struct {
	// General
	// TelemetryHost - Host to listen on for the prometheus exporter
	// TelemetryPort - Port to listen on for the prometheus exporter
	// LogLevel - Logger's log granularity (debug, info, warn, error, fatal, panic)
	// Version - Set by the dockerfile, will be logged once in the beginning
	TelemetryHost string `envconfig:"TELEMETRY_HOST" default:"0.0.0.0"`
	TelemetryPort int    `envconfig:"TELEMETRY_PORT" default:"8080"`
	LogLevel      string `envconfig:"LOG_LEVEL" default:"INFO"`
	Version       string `envconfig:"VERSION" required:"true"`

	// Exporter settings
	// IgnoreSystemTopics - Don't expose metrics about system topics (any topic names which are "__" or "_confluent" prefixed)
	IgnoreSystemTopics bool `envconfig:"EXPORTER_IGNORE_SYSTEM_TOPICS" default:"true"`

	// Kafka configurations
	// KafkaBrokers - Addresses of all Kafka Brokers delimited by comma (e. g. "kafka-1:9092, kafka-2:9092")
	// ConsumerOffsetsTopicName - Topic name of topic where kafka commits the consumer offsets
	// SASLEnabled - Bool to enable/disable SASL authentication (only SASL_PLAINTEXT is supported)
	// UseSASLHandshake -  Whether or not to send the Kafka SASL handshake first
	// SASLUsername - SASL Username
	// SASLPassword - SASL Password
	// TLSEnabled - Whether or not to use TLS when connecting to the broker
	// TLSCAFilePath - Path to the TLS CA file
	// TLSKeyFilePath - Path to the TLS Key file
	// TLSCertFilePath - Path to the TLS cert file
	// TLSInsecureSkipTLSVerify - If InsecureSkipVerify is true, TLS accepts any certificate presented by the server and any host name in that certificate.
	// TLSPassphrase - Passphrase to decrypt the TLS Key
	KafkaBrokers             []string `envconfig:"KAFKA_BROKERS" required:"true"`
	ConsumerOffsetsTopicName string   `envconfig:"KAFKA_CONSUMER_OFFSETS_TOPIC_NAME" default:"__consumer_offsets"`
	SASLEnabled              bool     `envconfig:"KAFKA_SASL_ENABLED" default:"false"`
	UseSASLHandshake         bool     `envconfig:"KAFKA_SASL_USE_HANDSHAKE" default:"true"`
	SASLUsername             string   `envconfig:"KAFKA_SASL_USERNAME"`
	SASLPassword             string   `envconfig:"KAFKA_SASL_PASSWORD"`
	TLSEnabled               bool     `envconfig:"KAFKA_TLS_ENABLED" default:"false"`
	TLSCAFilePath            string   `envconfig:"KAFKA_TLS_CA_FILE_PATH"`
	TLSKeyFilePath           string   `envconfig:"KAFKA_TLS_KEY_FILE_PATH"`
	TLSCertFilePath          string   `envconfig:"KAFKA_TLS_CERT_FILE_PATH"`
	TLSInsecureSkipTLSVerify bool     `envconfig:"KAFKA_TLS_INSECURE_SKIP_TLS_VERIFY" default:"true"`
	TLSPassphrase            string   `envconfig:"KAFKA_TLS_PASSPHRASE"`

	// Prometheus exporter
	// MetricsPrefix - A prefix for all exported prometheus metrics
	MetricsPrefix string `envconfig:"METRICS_PREFIX" default:"kafka_minion"`
}

// NewOptions provides Application Options
func NewOptions() *Options {
	return &Options{}
}
