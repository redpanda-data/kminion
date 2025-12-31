package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/cloudhut/kminion/v2/kafka"
	"github.com/cloudhut/kminion/v2/logging"
	"github.com/cloudhut/kminion/v2/minion"
	"github.com/cloudhut/kminion/v2/prometheus"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/mitchellh/mapstructure"
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
		key := strings.ReplaceAll(strings.ToLower(s), "_", ".")
		// Check to exist if we have a configuration option already and see if it's a slice
		// If there is a comma in the value, split the value into a slice by the comma.
		if strings.Contains(v, ",") {
			return key, strings.Split(v, ",")
		}

		// Otherwise return the new key with the unaltered value
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
