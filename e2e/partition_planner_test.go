package e2e

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// buildMeta constructs a MetadataResponse for tests.
// brokers: brokerID -> rack label ("" means no rack/unknown).
// partitions: list of replica lists where index 0 is the preferred leader.
func buildMeta(topic string, brokers map[int32]string, partitions [][]int32) *kmsg.MetadataResponse {
	// Brokers
	bs := make([]kmsg.MetadataResponseBroker, 0, len(brokers))
	ids := make([]int32, 0, len(brokers))
	for id := range brokers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		rack := brokers[id] // copy for address stability
		bs = append(bs, kmsg.MetadataResponseBroker{
			NodeID: id,
			Rack:   &rack, // empty string is allowed and treated as one "rack" bucket by planner
		})
	}

	// Partitions
	ps := make([]kmsg.MetadataResponseTopicPartition, 0, len(partitions))
	for i, reps := range partitions {
		cp := append([]int32(nil), reps...)
		leader := int32(-1)
		if len(reps) > 0 {
			leader = reps[0] // In tests, assume preferred leader is actual leader
		}
		ps = append(ps, kmsg.MetadataResponseTopicPartition{
			Partition: int32(i),
			Leader:    leader,
			Replicas:  cp,
		})
	}

	return &kmsg.MetadataResponse{
		Brokers: bs,
		Topics: []kmsg.MetadataResponseTopic{
			{
				Topic:      kmsg.StringPtr(topic),
				Partitions: ps,
			},
		},
	}
}

// applyPlan returns the final predictive assignments after applying the plan
// to the given metadata snapshot (without mutating meta).
func applyPlan(meta *kmsg.MetadataResponse, plan *Plan) map[int32][]int32 {
	final := map[int32][]int32{}
	for _, p := range meta.Topics[0].Partitions {
		final[p.Partition] = append([]int32(nil), p.Replicas...)
	}
	for _, ra := range plan.Reassignments {
		final[ra.Partition] = append([]int32(nil), ra.Replicas...)
	}
	nextID := int32(len(meta.Topics[0].Partitions))
	for _, ca := range plan.CreateAssignments {
		final[nextID] = append([]int32(nil), ca.Replicas...)
		nextID++
	}
	return final
}

func countLeaders(assigns map[int32][]int32) map[int32]int {
	m := map[int32]int{}
	for _, reps := range assigns {
		if len(reps) > 0 {
			m[reps[0]]++
		}
	}
	return m
}

func assertNoDuplicates(t *testing.T, reps []int32) {
	t.Helper()
	seen := map[int32]struct{}{}
	for _, r := range reps {
		_, dup := seen[r]
		assert.Falsef(t, dup, "duplicate broker in replica set: %v", reps)
		seen[r] = struct{}{}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- tests -----------------------------------------------------------------

func TestPartitionPlanner_Plan(t *testing.T) {
	type tc struct {
		name       string
		brokers    map[int32]string    // brokerID -> rack ("" for no rack)
		partitions [][]int32           // ordered replicas (index 0 = preferred leader)
		cfg        EndToEndTopicConfig // uses ReplicationFactor & PartitionsPerBroker
		check      func(t *testing.T, meta *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32)
	}

	tests := []tc{
		{
			name:       "single broker creates one partition",
			brokers:    map[int32]string{1: ""},
			partitions: nil, // empty topic
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   1,
				PartitionsPerBroker: 1,
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				require.NotNil(t, plan)
				assert.Equal(t, 1, len(plan.CreateAssignments), "should create exactly one partition")
				assert.Equal(t, 1, plan.FinalPartitionCount)

				for pid, reps := range final {
					assert.Lenf(t, reps, 1, "pid %d must have RF=1", pid)
				}
				leaders := countLeaders(final)
				assert.Equal(t, 1, leaders[1], "broker 1 should lead one partition")
			},
		},
		{
			name: "three brokers, no racks, RF grows to 3; ensure coverage and count",
			brokers: map[int32]string{
				1: "", 2: "", 3: "",
			},
			// start with 2 partitions at RF=2 both led by broker with id 1 (skewed)
			partitions: [][]int32{
				{1, 2},
				{1, 2},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   3,
				PartitionsPerBroker: 1,
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				assert.Equal(t, 3, plan.FinalPartitionCount, "desired should be max(cur=2, ppb*brokers=3)=3")

				for pid, reps := range final {
					assert.Lenf(t, reps, 3, "pid %d must have RF=3", pid)
					assertNoDuplicates(t, reps)
				}
				leaders := countLeaders(final)
				assert.GreaterOrEqual(t, leaders[1], 1)
				assert.GreaterOrEqual(t, leaders[2], 1)
				assert.GreaterOrEqual(t, leaders[3], 1)
			},
		},
		{
			name: "rack diversity improves on same-RF partition (2 racks, RF=2)",
			brokers: map[int32]string{
				1: "a", 2: "a", 3: "b",
			},
			// single partition with both replicas on rack "a" -> re-pick to include "b"
			partitions: [][]int32{
				{1, 2},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 1, // desired total becomes 3, but p0 should be improved first
			},
			check: func(t *testing.T, meta *kmsg.MetadataResponse, _ *Plan, final map[int32][]int32) {
				reps := final[0]
				require.Len(t, reps, 2)
				assertNoDuplicates(t, reps)

				state := BuildState(meta)
				assert.False(t, violatesRackDiversity(reps, state.NumRacks, state.Brokers), "p0 should span both racks a/b")
			},
		},
		{
			name: "shrink RF preserves current leader",
			brokers: map[int32]string{
				1: "", 2: "", 3: "",
			},
			// RF=3 currently, leader is 2; configured RF=2 -> leader must remain 2
			partitions: [][]int32{
				{2, 1, 3},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 1,
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, _ *Plan, final map[int32][]int32) {
				reps := final[0]
				require.Len(t, reps, 2)
				assert.Equal(t, int32(2), reps[0], "leader 2 should be preserved after shrink")
				assertNoDuplicates(t, reps)
			},
		},
		{
			name: "rotate-if-replica covers missing brokers without extra swaps",
			brokers: map[int32]string{
				1: "", 2: "", 3: "",
			},
			// 2 partitions, both led by 1; 2 and 3 are replicas only in separate partitions.
			partitions: [][]int32{
				{1, 2},
				{1, 3},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 1, // desired total 3 -> one create expected
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				assert.Equal(t, 1, len(plan.CreateAssignments), "one create due to partition target")
				leaders := countLeaders(final)
				assert.GreaterOrEqual(t, leaders[1], 1)
				assert.GreaterOrEqual(t, leaders[2], 1)
				assert.GreaterOrEqual(t, leaders[3], 1)
				for pid, reps := range final {
					assert.Lenf(t, reps, 2, "pid %d must have RF=2", pid)
					assertNoDuplicates(t, reps)
				}
			},
		},
		{
			name: "replace-duplicate-and-rotate when target broker is not a replica anywhere",
			brokers: map[int32]string{
				1: "a", 2: "a", 3: "b",
			},
			// 2 partitions both {1,2}; broker 3 is nowhere; donors exist (1 leads 2),
			// and duplicates exist (rack "a" twice) so we can safely swap in 3 and rotate.
			partitions: [][]int32{
				{1, 2},
				{1, 2},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 1, // desired total 3; coverage for 3 should be via swap+rotate
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, _ *Plan, final map[int32][]int32) {
				leaders := countLeaders(final)
				assert.GreaterOrEqual(t, leaders[3], 1, "broker 3 should lead at least one partition")
				for pid, reps := range final {
					assert.Lenf(t, reps, 2, "pid %d RF=2", pid)
					assertNoDuplicates(t, reps)
				}
			},
		},
		{
			name: "even split leaders when scaling partitions (ppb=2)",
			brokers: map[int32]string{
				1: "", 2: "", 3: "",
			},
			// Start with 3 partitions, each broker already leads one
			partitions: [][]int32{
				{1, 2},
				{2, 1},
				{3, 1},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 2, // desired = 2 * 3 = 6
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				assert.Equal(t, 6, plan.FinalPartitionCount)
				leaders := countLeaders(final)
				assert.Equal(t, 2, leaders[1])
				assert.Equal(t, 2, leaders[2])
				assert.Equal(t, 2, leaders[3])
			},
		},
		{
			name: "one defined rack for all brokers -> no diversity churn; no ops needed",
			brokers: map[int32]string{
				1: "a", 2: "a", 3: "a",
			},
			// Already meets RF and coverage and desired count == 3
			partitions: [][]int32{
				{1, 2},
				{2, 3},
				{3, 1},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 1,
			},
			check: func(t *testing.T, meta *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				assert.Equal(t, 0, len(plan.Reassignments), "no reassignments expected")
				assert.Equal(t, 0, len(plan.CreateAssignments), "no creates expected")
				state := BuildState(meta)
				// Diversity can't be improved (NumRacks=1)
				for pid, reps := range final {
					assert.Falsef(t, violatesRackDiversity(reps, state.NumRacks, state.Brokers), "pid %d should not violate with NumRacks=1", pid)
				}
			},
		},
		{
			name: "broker present but leading nothing (restarting?) gets coverage (via swap or create)",
			brokers: map[int32]string{
				1: "a", 2: "b", 3: "c",
			},
			// Two partitions led by 1 and 2; broker 3 has no replicas/leadership.
			partitions: [][]int32{
				{1, 2},
				{2, 1},
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 1, // desired = max(2, 3)=3 -> at least one create or a swap+rotate + create for count
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				assert.Equal(t, 3, plan.FinalPartitionCount)
				leaders := countLeaders(final)
				assert.GreaterOrEqual(t, leaders[1], 1)
				assert.GreaterOrEqual(t, leaders[2], 1)
				assert.GreaterOrEqual(t, leaders[3], 1)
				for pid, reps := range final {
					assert.Lenf(t, reps, 2, "pid %d RF=2", pid)
					assertNoDuplicates(t, reps)
				}
			},
		},
		{
			name: "phase 3 accounts for leaders created in phase 2 - no over-assignment",
			brokers: map[int32]string{
				1: "a", 2: "b", 3: "c", 4: "d",
			},
			// Start with 2 partitions, both led by broker 1
			// Brokers 2,3,4 have no leadership -> phase 2 will create partitions for them
			// Then phase 3 should NOT over-assign broker 1 when creating additional partitions
			partitions: [][]int32{
				{1, 2}, // broker 1 leads
				{1, 3}, // broker 1 leads (over-represented)
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   2,
				PartitionsPerBroker: 2, // 4*2=8 total desired, currently have 2, so need 6 more
			},
			check: func(t *testing.T, _ *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				// Should create 6 new partitions (3 for coverage in phase 2, 3 more for count in phase 3)
				assert.Equal(t, 8, plan.FinalPartitionCount, "should reach desired partition count of 8")
				assert.Equal(t, 6, len(plan.CreateAssignments), "should create 6 new partitions")

				leaders := countLeaders(final)

				// Each broker should lead exactly 2 partitions (8 total / 4 brokers = 2 each)
				// This test will fail if phase 3 doesn't account for leaders created in phase 2
				// because it will see broker 1 as leading 2 partitions and think it needs 0 more,
				// while it actually leads 2 from existing + potentially more from phase 2
				for brokerID := int32(1); brokerID <= 4; brokerID++ {
					assert.Equal(t, 2, leaders[brokerID],
						"broker %d should lead exactly 2 partitions (even distribution), but leads %d",
						brokerID, leaders[brokerID])
				}
			},
		},
		{
			name: "uses actual leader not preferred leader when they differ",
			brokers: map[int32]string{
				1: "a", 2: "b", 3: "c",
			},
			// This case simulates when the actual leader differs from preferred leader (replicas[0])
			// We'll manually construct metadata where leader != replicas[0]
			partitions: [][]int32{
				{1, 2, 3}, // preferred leader is 1, but we'll set actual leader to 2 in buildMetaWithLeader
				{2, 3, 1}, // preferred leader is 2, but we'll set actual leader to 3 in buildMetaWithLeader
			},
			cfg: EndToEndTopicConfig{
				ReplicationFactor:   3,
				PartitionsPerBroker: 1, // 3*1=3 total desired, have 2, need 1 more
			},
			check: func(t *testing.T, meta *kmsg.MetadataResponse, plan *Plan, final map[int32][]int32) {
				// With our manually set leaders (2, 3), broker 1 has no leadership
				// The planner should recognize this and either rotate leadership to broker 1
				// or create a new partition led by broker 1
				leaders := countLeaders(final)
				assert.GreaterOrEqual(t, leaders[1], 1, "broker 1 should lead at least one partition")
				assert.GreaterOrEqual(t, leaders[2], 1, "broker 2 should lead at least one partition")
				assert.GreaterOrEqual(t, leaders[3], 1, "broker 3 should lead at least one partition")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var meta *kmsg.MetadataResponse
			if tt.name == "uses actual leader not preferred leader when they differ" {
				// Special case: manually set leaders to differ from preferred leaders
				meta = buildMeta("probe", tt.brokers, tt.partitions)
				// Override the leaders: partition 0 actual leader = 2, partition 1 actual leader = 3
				meta.Topics[0].Partitions[0].Leader = 2 // preferred is 1, actual is 2
				meta.Topics[0].Partitions[1].Leader = 3 // preferred is 2, actual is 3
			} else {
				meta = buildMeta("probe", tt.brokers, tt.partitions)
			}

			planner := NewPartitionPlanner(tt.cfg, zap.NewNop())
			plan, err := planner.Plan(meta)
			require.NoError(t, err, "Plan() should not error")
			require.NotNil(t, plan, "Plan() returned nil plan")

			// Sanity: final >= max(current, ppb*brokers)
			expectedMin := max(len(meta.Topics[0].Partitions), tt.cfg.PartitionsPerBroker*len(tt.brokers))
			assert.GreaterOrEqual(t, plan.FinalPartitionCount, expectedMin, "final partition count must meet lower bound")

			// Apply and enforce universal invariants.
			final := applyPlan(meta, plan)
			for pid, reps := range final {
				assert.Lenf(t, reps, tt.cfg.ReplicationFactor, "pid %d RF mismatch", pid)
				assertNoDuplicates(t, reps)
			}

			// Scenario-specific checks.
			tt.check(t, meta, plan, final)
		})
	}
}

func TestPartitionPlanner_Plan_Deterministic(t *testing.T) {
	// Test that the same input produces identical plans across multiple runs
	brokers := map[int32]string{
		10: "rack1", 20: "rack2", 30: "rack3", 40: "rack1", 50: "rack2", 60: "rack3",
	}
	// Many partitions with suboptimal RF to force fixReplicationAndRack to iterate over map
	partitions := [][]int32{
		{10}, {20}, {30}, {40}, {50}, {60}, // RF=1, needs growth to 3
		{10, 20}, {20, 30}, {30, 40}, {40, 50}, // RF=2, needs growth to 3
		{50, 60, 10}, {60, 10, 20}, {10, 30, 40}, // RF=3, may need rack fixes
	}
	cfg := EndToEndTopicConfig{
		ReplicationFactor:   3,
		PartitionsPerBroker: 3, // 6*3=18 total desired, have 13, need 5 more
	}

	meta := buildMeta("probe", brokers, partitions)
	var plans []*Plan

	// Run the same plan many times to increase chance of hitting different map iteration orders
	for i := 0; i < 10; i++ {
		planner := NewPartitionPlanner(cfg, zap.NewNop())
		plan, err := planner.Plan(meta)
		require.NoError(t, err, "Plan() should not error on run %d", i)
		require.NotNil(t, plan, "Plan() returned nil plan on run %d", i)
		plans = append(plans, plan)
	}

	// All plans should be identical
	firstPlan := plans[0]
	for i := 1; i < len(plans); i++ {
		assert.Equal(t, len(firstPlan.Reassignments), len(plans[i].Reassignments),
			"run %d: reassignment count should be identical", i)
		assert.Equal(t, len(firstPlan.CreateAssignments), len(plans[i].CreateAssignments),
			"run %d: create count should be identical", i)
		assert.Equal(t, firstPlan.FinalPartitionCount, plans[i].FinalPartitionCount,
			"run %d: final partition count should be identical", i)

		// Build maps for comparison
		reassign1 := make(map[int32][]int32)
		for _, r := range firstPlan.Reassignments {
			reassign1[r.Partition] = r.Replicas
		}
		reassign2 := make(map[int32][]int32)
		for _, r := range plans[i].Reassignments {
			reassign2[r.Partition] = r.Replicas
		}
		assert.Equal(t, reassign1, reassign2, "run %d: reassignments should be identical", i)

		// Create assignments order matters for determinism
		assert.Equal(t, firstPlan.CreateAssignments, plans[i].CreateAssignments,
			"run %d: create assignments should be identical", i)
	}
}

// TestActualLeaderCoverageSkipsPreferredRebalancing tests the fix for the bug where
// ensureLeaderCoverage would trigger unnecessary reassignments when actual leader
// coverage was perfect but preferred leader coverage was unbalanced.
func TestActualLeaderCoverageSkipsPreferredRebalancing(t *testing.T) {
	// Simulate the exact scenario from the bug report:
	// - All brokers in same rack (no rack diversity benefit possible)
	// - Actual leaders perfectly distributed: broker 0→p1, broker 1→p0, broker 2→p2
	// - But preferred leaders (replicas[0]) unbalanced: broker 1→p0&p2, broker 0→p1, broker 2→none
	brokers := map[int32]string{
		0: "europe-west1-b", 1: "europe-west1-b", 2: "europe-west1-b",
	}

	// Build metadata with specific replica assignments matching the bug report
	meta := buildMeta("probe", brokers, [][]int32{
		{1, 2, 0}, // partition 0: preferred leader = 1
		{0, 1, 2}, // partition 1: preferred leader = 0
		{1, 2, 0}, // partition 2: preferred leader = 1
	})

	// Override actual leaders to match the bug report scenario
	meta.Topics[0].Partitions[0].Leader = 1 // p0: preferred=1, actual=1 (same)
	meta.Topics[0].Partitions[1].Leader = 0 // p1: preferred=0, actual=0 (same)
	meta.Topics[0].Partitions[2].Leader = 2 // p2: preferred=1, actual=2 (DIFFERENT!)

	cfg := EndToEndTopicConfig{
		ReplicationFactor:   3,
		PartitionsPerBroker: 1, // 3*1=3 total desired, have 3, perfect
	}

	planner := NewPartitionPlanner(cfg, zap.NewNop())
	plan, err := planner.Plan(meta)
	require.NoError(t, err, "Plan() should not error")
	require.NotNil(t, plan, "Plan() returned nil plan")

	// This is the key assertion: should have ZERO reassignments because:
	// 1. All brokers in same rack → no rack diversity violations possible
	// 2. Actual leader coverage already perfect (each broker leads exactly 1 partition)
	// 3. RF and partition count already correct
	assert.Equal(t, 0, len(plan.Reassignments), "should have no reassignments when actual coverage is perfect")
	assert.Equal(t, 0, len(plan.CreateAssignments), "should have no creates when partition count is perfect")
	assert.Equal(t, 3, plan.FinalPartitionCount, "should maintain existing partition count")

	// Verify the fix: no changes to replica assignments
	final := applyPlan(meta, plan)
	assert.Equal(t, []int32{1, 2, 0}, final[0], "partition 0 replicas should be unchanged")
	assert.Equal(t, []int32{0, 1, 2}, final[1], "partition 1 replicas should be unchanged")
	assert.Equal(t, []int32{1, 2, 0}, final[2], "partition 2 replicas should be unchanged")

	// Verify all partitions still have correct RF and no duplicates
	for pid, reps := range final {
		assert.Lenf(t, reps, 3, "pid %d must have RF=3", pid)
		assertNoDuplicates(t, reps)
	}
}
