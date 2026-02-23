package e2e

import (
	"fmt"
	"time"
)

type EndToEndTopicConfig struct {
	Enabled                bool          `koanf:"enabled"`
	Name                   string        `koanf:"name"`
	ReplicationFactor      int           `koanf:"replicationFactor"`
	PartitionsPerBroker    int           `koanf:"partitionsPerBroker"`
	ReconciliationInterval time.Duration `koanf:"reconciliationInterval"`
	// RebalancePartitions controls whether kminion will issue AlterPartitionAssignments
	// requests to rebalance partition leaders across brokers. Set to false when running
	// against Kafka-compatible clusters (e.g. Redpanda with autobalancer enabled, Confluent
	// Cloud) that reject AlterPartitionAssignments. Topic creation and partition count
	// management remain active regardless of this setting.
	RebalancePartitions bool `koanf:"rebalancePartitions"`
}

func (c *EndToEndTopicConfig) SetDefaults() {
	c.Enabled = true
	c.Name = "kminion-end-to-end"
	c.ReplicationFactor = 1
	c.PartitionsPerBroker = 1
	c.ReconciliationInterval = 10 * time.Minute
	c.RebalancePartitions = true
}

func (c *EndToEndTopicConfig) Validate() error {

	if c.ReplicationFactor < 1 {
		return fmt.Errorf("failed to parse replicationFactor, it should be more than 1, retrieved value %v", c.ReplicationFactor)
	}

	if c.PartitionsPerBroker < 1 {
		return fmt.Errorf("failed to parse partitionsPerBroker, it should be more than 1, retrieved value %v", c.PartitionsPerBroker)
	}

	// If the timeduration is 0s or 0ms or its variation of zero, it will be parsed as 0
	if c.ReconciliationInterval == 0 {
		return fmt.Errorf("failed to validate topic.ReconciliationInterval config, the duration can't be zero")
	}

	return nil
}
