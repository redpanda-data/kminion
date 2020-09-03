package options

import "time"

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
	// TopicFilter - Regex used to filter which topics to include metrics for
	IgnoreSystemTopics bool   `envconfig:"EXPORTER_IGNORE_SYSTEM_TOPICS" default:"true"`
	TopicFilter        string `envconfig:"EXPORTER_TOPIC_FILTER" default:".*"`

	// Kafka configurations
	// KafkaBrokers - Addresses of all Kafka Brokers delimited by comma (e. g. "kafka-1:9092, kafka-2:9092")
	// ConsumerOffsetsTopicName - Topic name of topic where kafka commits the consumer offsets use TLS when connecting to the broker
	KafkaBrokers             []string      `envconfig:"KAFKA_BROKERS" required:"true"`
	KafkaVersion             string        `envconfig:"KAFKA_VERSION" default:"1.0.0"`
	OffsetRetention          time.Duration `envconfig:"KAFKA_OFFSET_RETENTION" default:"168h"` // 7d default
	ConsumerOffsetsTopicName string        `envconfig:"KAFKA_CONSUMER_OFFSETS_TOPIC_NAME" default:"__consumer_offsets"`

	// SASL Settings
	// SASLEnabled - Bool to enable/disable SASL authentication (only SASL_PLAINTEXT is supported)
	// SASLMechanism - Default to empty : set to SCRAM-SHA-256 or SCRAM-SHA-512 for scram usage,
	// UseSASLHandshake -  Whether or not to send the Kafka SASL handshake first
	// SASLUsername - SASL Username
	// SASLPassword - SASL Password
	// TLSEnabled - Whether or not to
	SASLEnabled      bool   `envconfig:"KAFKA_SASL_ENABLED" default:"false"`
	SASLMechanism    string `envconfig:"KAFKA_SASL_MECHANISM" default:""`
	UseSASLHandshake bool   `envconfig:"KAFKA_SASL_USE_HANDSHAKE" default:"true"`
	SASLUsername     string `envconfig:"KAFKA_SASL_USERNAME"`
	SASLPassword     string `envconfig:"KAFKA_SASL_PASSWORD"`
	// Kerberos Settings
	// SASLGSSAPIAuthType - Auth type (either "USER_AUTH" or "KEYTAB_AUTH")
	SASLGSSAPIAuthType           string `envconfig:"KAFKA_SASL_GSSAPI_AUTH_TYPE"`
	SASLGSSAPIKeyTabPath         string `envconfig:"KAFKA_SASL_GSSAPI_KEY_TAB_PATH"`
	SASLGSSAPIKerberosConfigPath string `envconfig:"KAFKA_SASL_GSSAPI_KERBEROS_CONFIG_PATH"`
	SASLGSSAPIServiceName        string `envconfig:"KAFKA_SASL_GSSAPI_SERVICE_NAME"`
	SASLGSSAPIUsername           string `envconfig:"KAFKA_SASL_GSSAPI_USERNAME"`
	SASLGSSAPIPassword           string `envconfig:"KAFKA_SASL_GSSAPI_PASSWORD"`
	SASLGSSAPIRealm              string `envconfig:"KAFKA_SASL_GSSAPI_REALM"`

	// TLS Settings
	// TLSEnabled - Whether or not to use TLS encryption
	// TLSCAFilePath - Path to the TLS CA file
	// TLSKeyFilePath - Path to the TLS Key file
	// TLSCertFilePath - Path to the TLS cert file
	// TLSInsecureSkipTLSVerify - If InsecureSkipVerify is true, TLS accepts any certificate presented by the server and any host name in that certificate.
	// TLSPassphrase - Passphrase to decrypt the TLS Key
	TLSEnabled               bool   `envconfig:"KAFKA_TLS_ENABLED" default:"false"`
	TLSCAFilePath            string `envconfig:"KAFKA_TLS_CA_FILE_PATH"`
	TLSKeyFilePath           string `envconfig:"KAFKA_TLS_KEY_FILE_PATH"`
	TLSCertFilePath          string `envconfig:"KAFKA_TLS_CERT_FILE_PATH"`
	TLSInsecureSkipTLSVerify bool   `envconfig:"KAFKA_TLS_INSECURE_SKIP_TLS_VERIFY" default:"true"`
	TLSPassphrase            string `envconfig:"KAFKA_TLS_PASSPHRASE"`

	// Prometheus exporter
	// MetricsPrefix - A prefix for all exported prometheus metrics
	MetricsPrefix string `envconfig:"METRICS_PREFIX" default:"kafka_minion"`
}

// NewOptions provides Application Options
func NewOptions() *Options {
	return &Options{}
}
