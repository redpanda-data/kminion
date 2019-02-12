package options

// Options are configuration options that can be set by Environment Variables
// Version - Application version
// Kafka Broker string
type Options struct {
	// General
	// Port - Port to listen on for the prometheus exporter
	// LogLevel - Logger's log granularity (debug, info, warn, error, fatal, panic)
	// Version - Set by the dockerfile, will be logged once in the beginning
	Port     int    `envconfig:"PORT" default:"8080"`
	LogLevel string `envconfig:"LOG_LEVEL" default:"INFO"`
	Version  string `envconfig:"VERSION" required:"true"`

	// Kafka
	// KafkaBrokers - Addresses of all Kafka Brokers delimited by comma (e. g. "kafka-1:9092, kafka-2:9092")
	// SASLEnabled - Bool to enable/disable SASL authentication (only SASL_PLAINTEXT is supported)
	// UseSASLHandshake -  Whether or not to send the Kafka SASL handshake first
	// SASLUsername - SASL Username
	// SASLPassword - SASL Password
	// TLSEnabled - Whether or not to use TLS when connecting to the broker
	// TLSCAFilePath - Path to the TLS CA file
	// TLSKeyFilePath - Path to the TLS Key file
	// TLSCertFilePath - Path to the TLS cert file
	// TLSInsecureSkipTLSVerify - If InsecureSkipVerify is true, TLS accepts any certificate presented by the server and any host name in that certificate.
	KafkaBrokers             []string `envconfig:"KAFKA_BROKERS" required:"true"`
	SASLEnabled              bool     `envconfig:"SASL_ENABLED" default:"false"`
	UseSASLHandshake         bool     `envconfig:"SASL_USE_HANDSHAKE" default:"true"`
	SASLUsername             string   `envconfig:"SASL_USERNAME"`
	SASLPassword             string   `envconfig:"SASL_PASSWORD"`
	TLSEnabled               bool     `envconfig:"TLS_ENABLED" default:"false"`
	TLSCAFilePath            string   `envconfig:"TLS_CA_FILE_PATH"`
	TLSKeyFilePath           string   `envconfig:"TLS_KEY_FILE_PATH"`
	TLSCertFilePath          string   `envconfig:"TLS_CERT_FILE_PATH"`
	TLSInsecureSkipTLSVerify bool     `envconfig:"TLS_INSECURE_SKIP_TLS_VERIFY" default:"true"`
	TLSPassphrase            string   `envconfig:"TLS_PASSPHRASE"`

	// Prometheus exporter
	// MetricsPrefix - A prefix for all exported prometheus metrics
	MetricsPrefix string `envconfig:"METRICS_PREFIX" default:"kafka_minion"`
}

// NewOptions provides Application Options
func NewOptions() *Options {
	return &Options{}
}
