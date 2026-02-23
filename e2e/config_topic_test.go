package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEndToEndTopicConfig_SetDefaults(t *testing.T) {
	var cfg EndToEndTopicConfig
	cfg.SetDefaults()

	assert.True(t, cfg.Enabled)
	assert.Equal(t, "kminion-end-to-end", cfg.Name)
	assert.Equal(t, 1, cfg.ReplicationFactor)
	assert.Equal(t, 1, cfg.PartitionsPerBroker)
	assert.Equal(t, 10*time.Minute, cfg.ReconciliationInterval)
	assert.True(t, cfg.RebalancePartitions, "RebalancePartitions should default to true for backward compatibility")
}

func TestEndToEndTopicConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     EndToEndTopicConfig
		wantErr bool
	}{
		{
			name: "valid config with rebalance enabled",
			cfg: EndToEndTopicConfig{
				Enabled:                true,
				Name:                   "test-topic",
				ReplicationFactor:      3,
				PartitionsPerBroker:    1,
				ReconciliationInterval: 10 * time.Minute,
				RebalancePartitions:    true,
			},
			wantErr: false,
		},
		{
			name: "valid config with rebalance disabled",
			cfg: EndToEndTopicConfig{
				Enabled:                true,
				Name:                   "test-topic",
				ReplicationFactor:      3,
				PartitionsPerBroker:    1,
				ReconciliationInterval: 10 * time.Minute,
				RebalancePartitions:    false,
			},
			wantErr: false,
		},
		{
			name: "invalid replication factor",
			cfg: EndToEndTopicConfig{
				ReplicationFactor:      0,
				PartitionsPerBroker:    1,
				ReconciliationInterval: 10 * time.Minute,
				RebalancePartitions:    false,
			},
			wantErr: true,
		},
		{
			name: "invalid partitions per broker",
			cfg: EndToEndTopicConfig{
				ReplicationFactor:      1,
				PartitionsPerBroker:    0,
				ReconciliationInterval: 10 * time.Minute,
				RebalancePartitions:    false,
			},
			wantErr: true,
		},
		{
			name: "zero reconciliation interval",
			cfg: EndToEndTopicConfig{
				ReplicationFactor:      1,
				PartitionsPerBroker:    1,
				ReconciliationInterval: 0,
				RebalancePartitions:    false,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestPartitionPlanner_RebalancePartitionsDisabled verifies that when
// RebalancePartitions is false the planner still produces a valid plan
// (the caller in topic.go is responsible for not executing reassignments),
// and that partition creation assignments are still generated normally.
func TestPartitionPlanner_RebalancePartitionsDisabled(t *testing.T) {
	// Three brokers, topic already exists with suboptimal leader distribution:
	// all partitions led by broker 0.
	meta := buildMeta("e2e",
		map[int32]string{0: "", 1: "", 2: ""},
		[][]int32{
			{0, 1, 2},
			{0, 2, 1},
			{0, 1, 2},
		},
	)

	cfg := EndToEndTopicConfig{
		Enabled:                true,
		Name:                   "e2e",
		ReplicationFactor:      3,
		PartitionsPerBroker:    1,
		ReconciliationInterval: 10 * time.Minute,
		RebalancePartitions:    false,
	}

	planner := NewPartitionPlanner(cfg, zap.NewNop())
	plan, err := planner.Plan(meta)
	require.NoError(t, err)

	// The planner should still detect that reassignments are needed —
	// it's the caller's responsibility to skip executing them.
	// We just assert the plan itself is structurally valid.
	for _, ra := range plan.Reassignments {
		assertNoDuplicates(t, ra.Replicas)
		assert.Len(t, ra.Replicas, cfg.ReplicationFactor)
	}

	// No new partitions should be created (3 brokers × 1 per broker = 3 already exist).
	assert.Empty(t, plan.CreateAssignments)
	assert.Equal(t, 3, plan.FinalPartitionCount)
}
