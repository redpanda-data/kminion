package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/mitchellh/mapstructure"
	"github.com/redpanda-data/kminion/v2/kafka"
	"github.com/redpanda-data/kminion/v2/logging"
	"github.com/redpanda-data/kminion/v2/minion"
	"github.com/redpanda-data/kminion/v2/prometheus"
	"go.uber.org/zap"
)

type Config struct {
	Kafka    kafka.Config      `koanf:"kafka"`
	Minion   minion.Config     `koanf:"minion"`
	Exporter prometheus.Config `koanf:"exporter"`
	Logger   logging.Config    `koanf:"logger"`
}

func (c *Config) SetDefaults() {
	c.Kafka.SetDefaults()
	c.Minion.SetDefaults()
	c.Exporter.SetDefaults()
	c.Logger.SetDefaults()
}

func (c *Config) Validate() error {
	err := c.Kafka.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate kafka config: %w", err)
	}

	err = c.Minion.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate minion config: %w", err)
	}

	err = c.Logger.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate logger config: %w", err)
	}

	return nil
}

func newConfig(logger *zap.Logger) (Config, error) {
	k := koanf.New(".")
	var cfg Config
	cfg.SetDefaults()

	// 1. Check if a config filepath is set via flags. If there is one we'll try to load the file using a YAML Parser
	envKey := "CONFIG_FILEPATH"
	configFilepath := os.Getenv(envKey)
	if configFilepath == "" {
		logger.Info("the env variable '" + envKey + "' is not set, therefore no YAML config will be loaded")
	} else {
		err := k.Load(file.Provider(configFilepath), yaml.Parser())
		if err != nil {
			return Config{}, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	}

	// We could unmarshal the loaded koanf input after loading both providers, however we want to unmarshal the YAML
	// config with `ErrorUnused` set to true, but unmarshal environment variables with `ErrorUnused` set to false (default).
	// Rationale: Orchestrators like Kubernetes inject unrelated environment variables, which we still want to allow.
	err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{
		Tag:       "",
		FlatPaths: false,
		DecoderConfig: &mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc()),
			Metadata:         nil,
			Result:           &cfg,
			WeaklyTypedInput: true,
			ErrorUnused:      true,
		},
	})
	if err != nil {
		return Config{}, err
	}

	err = k.Load(env.ProviderWithValue("", ".", func(s string, v string) (string, interface{}) {
		if isServiceLinkEnvVar(s, v) {
			// An empty key makes koanf skip the env var.
			return "", nil
		}
		key := strings.Replace(strings.ToLower(s), "_", ".", -1)
		// Keep the raw value. The default decode hook splits comma-separated values only for slice fields.
		return key, v
	}), nil)
	if err != nil {
		return Config{}, err
	}

	err = k.Unmarshal("", &cfg)
	if err != nil {
		return Config{}, err
	}

	err = cfg.Validate()
	if err != nil {
		return Config{}, fmt.Errorf("failed to validate config: %w", err)
	}

	return cfg, nil
}

// serviceLinkEnvVarName matches the names of the env vars that Kubernetes injects for every Service in the namespace,
// except <SERVICE>_PORT. See https://kubernetes.io/docs/concepts/services-networking/service/#environment-variables
var serviceLinkEnvVarName = regexp.MustCompile(`_SERVICE_HOST$|_SERVICE_PORT(_[A-Z0-9_]+)?$|_PORT_[0-9]+_(TCP|UDP|SCTP)(_PROTO|_PORT|_ADDR)?$`)

// isServiceLinkEnvVar reports whether an env var is a Kubernetes service link. No config key matches these names,
// but they can turn a config key into a map, which makes decoding fail.
func isServiceLinkEnvVar(name, value string) bool {
	if serviceLinkEnvVarName.MatchString(name) {
		return true
	}
	// <SERVICE>_PORT has a URL value, which a real config value such as EXPORTER_PORT=8080 never has.
	return strings.HasSuffix(name, "_PORT") &&
		(strings.HasPrefix(value, "tcp://") || strings.HasPrefix(value, "udp://") || strings.HasPrefix(value, "sctp://"))
}
