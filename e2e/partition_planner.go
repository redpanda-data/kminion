package e2e

import (
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// PartitionPlanner is a thin orchestrator around the three planning phases. It wires in
// configuration, logging, and the replica-selection strategy. The planning phases are:
//  1. Fix replication factor and rack diversity on existing partitions.
//  2. Ensure every broker is the preferred leader of at least 1 partition.
//  3. Ensure total partition count meets the configured lower bound.
//
// The probe topic is created to continuously test end-to-end availability by
// producing and consuming records per partition. The planner enforces:
//
//   - Correct replication factor (RF == configured RF) and no duplicate brokers
//     within a single partition's replica set.
//   - Rack awareness: maximize the number of unique racks per partition (bounded
//     by min(RF, #racks)). This reduces the blast radius of a rack failure.
//   - Sufficient partition count: >= max(current, #brokers, partitionsPerBroker*#brokers).
//   - Leader coverage: every broker must be the preferred leader (replicas[0]) of
//     at least one partition, so a per-broker failure is observable.
//   - Minimal movement: prefer to fix RF and rack issues first, then rotate/swap
//     leaders to fill gaps, and only create partitions when necessary.
type PartitionPlanner struct {
	cfg    EndToEndTopicConfig
	logger *zap.Logger
	sel    ReplicaSelector
}

// NewPartitionPlanner constructs a Planner with the given config & logger. The replica
// selector is chosen in Plan() once we have ClusterState ready.
func NewPartitionPlanner(cfg EndToEndTopicConfig, logger *zap.Logger) *PartitionPlanner {
	return &PartitionPlanner{cfg: cfg, logger: logger}
}

// Plan produces an in-memory plan (reassignments + creations) for the probe
// topic based on current cluster metadata. See the package header for the
// invariants we enforce.
func (p *PartitionPlanner) Plan(meta *kmsg.MetadataResponse) (*Plan, error) {
	if meta == nil || len(meta.Topics) == 0 {
		return nil, fmt.Errorf("metadata response has no topics")
	}
	if len(meta.Brokers) == 0 {
		return nil, fmt.Errorf("metadata response has no brokers")
	}
	if p.cfg.ReplicationFactor > len(meta.Brokers) {
		return nil, fmt.Errorf("replication factor %d exceeds available brokers %d", p.cfg.ReplicationFactor, len(meta.Brokers))
	}

	// Build state required for the planning
	state := BuildState(meta)
	desired := ComputeDesired(state, p.cfg)
	tracker := NewLoadTracker(state)
	selector := NewRackAwareSelector(state, tracker)
	p.sel = selector

	b := NewPlanBuilder(state, desired, tracker, p.cfg.RebalancePartitions)

	// Phase 1: normalize RF and racks (low movement first)
	// Grow/trim replicas to configured RF and re-pick to maximize unique racks
	// per partition. We avoid moving the leader when possible.
	fixReplicationAndRack(b, selector, p.cfg.ReplicationFactor)

	// Phase 2: ensure each broker is preferred leader for >= 1 partition
	// This guarantees probe coverage: if a broker dies, some partition leader is
	// unavailable and the probe trips.
	ensureLeaderCoverage(b, selector)

	// Phase 3: ensure desired partition count
	// Enforce >= max(current, #brokers, partitionsPerBroker * #brokers).
	ensurePartitionCount(b, selector)

	// Build final plan and log the changes
	plan := b.Build()

	// Log with appropriate level based on whether changes are needed
	totalChanges := len(plan.Reassignments) + len(plan.CreateAssignments)
	if totalChanges == 0 {
		p.logger.Info("e2e probe topic partition leadership and replica distribution check completed - optimal",
			zap.String("topic", state.TopicName),
			zap.Int("brokers", len(state.Brokers)),
			zap.Int("partitions", len(state.Partitions)),
			zap.String("status", "optimal"),
		)
	} else {
		p.logger.Info("plan to change partition leadership and replica placements on e2e topic has been prepared",
			zap.String("topic", state.TopicName),
			zap.Int("brokers", len(state.Brokers)),
			zap.Int("cur_partitions", len(state.Partitions)),
			zap.Int("final_partitions", plan.FinalPartitionCount),
			zap.Int("reassignments", len(plan.Reassignments)),
			zap.Int("creates", len(plan.CreateAssignments)),
			zap.Int("total_changes", totalChanges),
		)
	}
	return plan, nil
}

// -----------------------------------------------------------------------------
// Planning phases
// -----------------------------------------------------------------------------

// PlanBuilder holds a predictive view of partition -> replicas after applying
// staged operations. We never mutate ClusterState.Partitions; instead, we write
// new assignments into PlanBuilder.view and record high-level operations to
// produce Kafka requests at the end (see Plan.ToRequests).
//
// This keeps the planning phases simple and side-effect free.
type PlanBuilder struct {
	state   ClusterState
	desired Desired
	tracker *LoadTracker

	// rebalancePartitions indicates whether reassignments will actually be executed.
	// When false, Phase 3 uses actual current leaders instead of predicted leaders
	// from the view, since reassignments won't be applied.
	rebalancePartitions bool

	// view is our predictive map: partitionID -> replicas (preferred leader at idx 0)
	view map[int32][]int32

	reassignments []Reassignment     // staged reassignments for existing partitions
	creations     []CreateAssignment // staged creations of new partitions
}

// Reassignment captures a single partition’s new replica list.
//
// The order of Replicas matters: index 0 will become the preferred leader after
// reassignment completes on the broker side.
type Reassignment struct {
	Partition int32
	Replicas  []int32
}

// CreateAssignment captures the replica list for a *new* partition that will be
// appended to the topic during CreatePartitions.
//
// The order of Replicas matters: index 0 is the preferred leader for the new
// partition.
type CreateAssignment struct {
	Replicas []int32
}

// Plan is the final, immutable result of planning. It can be turned into Kafka
// requests via ToRequests.
//
// FinalPartitionCount is the topic’s partition count after applying creations.
// (Reassignments do not change the count.)
type Plan struct {
	Reassignments       []Reassignment
	CreateAssignments   []CreateAssignment
	FinalPartitionCount int
}

// NewPlanBuilder initializes a predictive view by cloning the current
// partition->replicas map. We avoid accidental mutation by copying slices.
func NewPlanBuilder(state ClusterState, desired Desired, tracker *LoadTracker, rebalancePartitions bool) *PlanBuilder {
	view := make(map[int32][]int32, len(state.Partitions))
	for pid, p := range state.Partitions {
		view[pid] = append([]int32(nil), p.Replicas...)
	}
	return &PlanBuilder{state: state, desired: desired, tracker: tracker, rebalancePartitions: rebalancePartitions, view: view}
}

// Build freezes the current staged operations into a Plan. We compute the final
// partition count as current + number of creates.
func (b *PlanBuilder) Build() *Plan {
	return &Plan{
		Reassignments:       b.reassignments,
		CreateAssignments:   b.creations,
		FinalPartitionCount: len(b.state.Partitions) + len(b.creations),
	}
}

// CommitReassignment records a reassignment and updates the predictive view.
func (b *PlanBuilder) CommitReassignment(pid int32, reps []int32) {
	b.reassignments = append(b.reassignments, Reassignment{Partition: pid, Replicas: reps})
	b.view[pid] = reps
}

// CommitCreate records a new-partition assignment. The final partition count is
// computed when building the Plan.
func (b *PlanBuilder) CommitCreate(reps []int32) {
	b.creations = append(b.creations, CreateAssignment{Replicas: reps})
}

// fixReplicationAndRack enforces configured RF on each existing partition
// (growing or shrinking as needed) and re-picks replicas when rack diversity can
// be improved. We try to keep the current leader by always retaining replicas[0]
// when shrinking.
func fixReplicationAndRack(b *PlanBuilder, sel ReplicaSelector, rf int) {
	// Sort partition IDs for deterministic iteration
	pids := make([]int32, 0, len(b.view))
	for pid := range b.view {
		pids = append(pids, pid)
	}
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })

	for _, pid := range pids {
		replicas := b.view[pid]
		desiredRF := rf
		newReplicas := replicas

		switch {
		// Grow: re-pick the full set based on the current actual leader.
		case len(replicas) < desiredRF:
			// Use the actual current leader from the partition metadata
			currentPartition, exists := b.state.Partitions[pid]
			preferredLeader := replicas[0] // fallback to preferred leader
			if exists && currentPartition.Leader != -1 {
				preferredLeader = currentPartition.Leader
			}
			newReplicas = sel.ChooseReplicas(preferredLeader, desiredRF)

		// Shrink: keep leader; then pick remaining replicas preferring
		// new racks, then lower load, then lower broker ID.
		case len(replicas) > desiredRF:
			newReplicas = shrinkPreservingLeader(b, pid, replicas, desiredRF)

		// Same RF: if rack diversity can be improved, re-pick.
		default:
			if violatesRackDiversity(replicas, b.state.NumRacks, b.state.Brokers) {
				// Use the actual current leader from the partition metadata
				currentPartition, exists := b.state.Partitions[pid]
				preferredLeader := replicas[0] // fallback to preferred leader
				if exists && currentPartition.Leader != -1 {
					preferredLeader = currentPartition.Leader
				}
				newReplicas = sel.ChooseReplicas(preferredLeader, desiredRF)
			}
		}

		if !equalInt32s(newReplicas, replicas) {
			b.CommitReassignment(pid, newReplicas)
		}
	}
}

// shrinkPreservingLeader returns a replica set of size rf that keeps the
// current leader and greedily prefers candidates that add a new rack;
// among equals, chooses lower load, then lower broker ID.
func shrinkPreservingLeader(b *PlanBuilder, pid int32, replicas []int32, rf int) []int32 {
	// Use the actual current leader from the partition metadata, not replicas[0]
	currentPartition, exists := b.state.Partitions[pid]
	leader := replicas[0] // fallback to preferred leader if no current leader found
	if exists && currentPartition.Leader != -1 {
		leader = currentPartition.Leader
	}
	keep := []int32{leader}

	seen := map[string]struct{}{
		b.state.Brokers[leader].Rack: {},
	}

	type cand struct {
		id   int32
		rack string
		load int
	}

	// Build the candidate pool from non-leader replicas.
	pool := make([]cand, 0, len(replicas)-1)
	for _, id := range replicas[1:] {
		pool = append(pool, cand{
			id:   id,
			rack: b.state.Brokers[id].Rack,
			load: b.tracker.Load(id).Replicas,
		})
	}

	// Greedy selection with dynamic "seen racks".
	for len(keep) < rf && len(pool) > 0 {
		best := 0
		for i := 1; i < len(pool); i++ {
			a, b2 := pool[i], pool[best]
			_, aSeen := seen[a.rack]
			_, bSeen := seen[b2.rack]

			switch {
			// Prefer a candidate that adds a new rack.
			case aSeen != bSeen:
				if !aSeen && bSeen {
					best = i
				}
			// Then prefer lower load.
			case a.load != b2.load:
				if a.load < b2.load {
					best = i
				}
			// Then prefer lower broker ID (stable tie-breaker).
			case a.id < b2.id:
				best = i
			}
		}

		chosen := pool[best]
		keep = append(keep, chosen.id)
		seen[chosen.rack] = struct{}{}

		// Remove chosen from pool (swap-delete).
		pool[best] = pool[len(pool)-1]
		pool = pool[:len(pool)-1]
	}

	return keep
}

// ensureLeaderCoverage guarantees that each broker becomes preferred leader for
// at least one partition. We try the cheapest options first:
//  1. If the broker already hosts a replica of some partition where it is not
//     leader, rotate it to index 0 (no RF change, minimal movement).
//  2. Otherwise, replace a non-unique-rack replica in some donor partition and
//     rotate the target broker to index 0.
//  3. If neither is possible, create a new partition led by the target broker.
func ensureLeaderCoverage(b *PlanBuilder, sel ReplicaSelector) {
	// Guard: if actual leaders already cover all brokers, skip preferred leader rebalancing
	actualLeaders := make(map[int32][]int32, len(b.state.BrokerIDs))
	for _, id := range b.state.BrokerIDs {
		actualLeaders[id] = nil
	}
	for pid, part := range b.state.Partitions {
		if part.Leader != -1 {
			actualLeaders[part.Leader] = append(actualLeaders[part.Leader], pid)
		}
	}
	if len(brokersMissingLeadership(b.state.BrokerIDs, actualLeaders)) == 0 {
		return // Actual coverage is perfect - no need to rebalance preferred leaders
	}

	// Build "leadersByBroker": broker -> list of partition IDs it currently leads (preferred).
	leadersByBroker := indexLeaders(b.state.BrokerIDs, b.view)

	// Brokers that currently lead zero partitions (preferred).
	// However, if a broker already has actual leadership (even if not preferred),
	// we can skip it to minimize unnecessary reassignments.
	missing := []int32{}
	for _, broker := range brokersMissingLeadership(b.state.BrokerIDs, leadersByBroker) {
		// Skip if this broker already has actual leadership
		if len(actualLeaders[broker]) > 0 {
			continue
		}
		missing = append(missing, broker)
	}
	if len(missing) == 0 {
		return
	}

	// Local helpers that both perform the action and update leadersByBroker.
	rotateIfReplica := func(target int32, donors []int32) bool {
		for _, donor := range donors {
			// Collect candidate partitions where target is already a replica
			pids := append([]int32(nil), leadersByBroker[donor]...)

			// Sort with preference: partitions where the donor is the ACTUAL leader first.
			// This ensures we're actually freeing up leadership from the donor, rather than
			// rotating a partition where the donor is only the preferred leader.
			// Then by partition ID for determinism.
			sort.Slice(pids, func(i, j int) bool {
				pi, pj := pids[i], pids[j]

				// Prefer partitions where the donor is the actual leader
				iDonorIsActual := b.state.Partitions[pi].Leader == donor
				jDonorIsActual := b.state.Partitions[pj].Leader == donor

				if iDonorIsActual != jDonorIsActual {
					return iDonorIsActual
				}
				// Then by partition ID for stability
				return pi < pj
			})

			for _, pid := range pids {
				reps := b.view[pid]
				if !contains(reps, target) {
					continue
				}
				newReps := putFirst(reps, target) // make target the leader
				b.CommitReassignment(pid, newReps)

				// Update bookkeeping: pid moves from donor to target.
				leadersByBroker[donor] = remove(leadersByBroker[donor], pid)
				leadersByBroker[target] = append(leadersByBroker[target], pid)
				return true
			}
		}
		return false
	}

	replaceDuplicateAndRotate := func(target int32, donors []int32) bool {
		for _, donor := range donors {
			// Sort partition IDs for deterministic iteration
			pids := append([]int32(nil), leadersByBroker[donor]...)
			sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
			for _, pid := range pids {
				reps := b.view[pid]
				if contains(reps, target) {
					continue // covered by rotate path above
				}
				idx := victimIndex(reps, b.state.Brokers)
				if idx < 0 {
					continue // no safe replica to swap (unique racks already)
				}
				newReps := append([]int32{}, reps...)
				newReps[idx] = target
				newReps = putFirst(newReps, target)
				b.CommitReassignment(pid, newReps)

				leadersByBroker[donor] = remove(leadersByBroker[donor], pid)
				leadersByBroker[target] = append(leadersByBroker[target], pid)
				return true
			}
		}
		return false
	}

	for _, target := range missing {
		// Donors: brokers leading more partitions than their soft target,
		// sorted by largest surplus first (tie-breaker: smaller broker ID).
		donors := donorBrokers(b.state.BrokerIDs, leadersByBroker, b.desired.TargetLeaders)

		// 1) Cheapest: rotate target to lead where it already is a replica.
		if rotateIfReplica(target, donors) {
			continue
		}
		// 2) Next-cheapest: replace a duplicate-rack replica, then rotate.
		if replaceDuplicateAndRotate(target, donors) {
			continue
		}
		// 3) Last resort: create a new partition led by target.
		reps := sel.ChooseReplicas(target, b.desired.RF)
		b.CommitCreate(reps)

		// Track a synthetic partition ID so counts stay consistent within this loop.
		newPID := int32(len(b.state.Partitions) + len(b.creations) - 1)
		leadersByBroker[target] = append(leadersByBroker[target], newPID)
	}
}

// ensurePartitionCount adds partitions until we reach DesiredPartitions.
//
// Leader selection strategy per new partition:
//  1. Prefer a broker still below its soft target (TargetLeaders). Among those,
//     pick the one closest to its target (smallest positive gap).
//  2. If all brokers are at/above target, pick the broker that currently leads
//     the fewest partitions (stable tie-breaker via leastLoadedLeader).
func ensurePartitionCount(b *PlanBuilder, sel ReplicaSelector) {
	desiredTotal := b.desired.DesiredPartitions
	total := len(b.state.Partitions) + len(b.creations)
	if total >= desiredTotal || len(b.state.BrokerIDs) == 0 {
		return
	}

	// Count current leaders per broker.
	leaderCount := make(map[int32]int, len(b.state.BrokerIDs))
	if b.rebalancePartitions {
		// Use predictive view (reassignments will be applied)
		for _, reps := range b.view {
			if len(reps) > 0 {
				leaderCount[reps[0]]++
			}
		}
	} else {
		// Use actual current leaders (reassignments won't be applied)
		for _, p := range b.state.Partitions {
			if p.Leader != -1 {
				leaderCount[p.Leader]++
			}
		}
	}
	// Always include leaders from staged creates (Phase 2 fallback creates are always executed)
	for _, ca := range b.creations {
		if len(ca.Replicas) > 0 {
			leaderCount[ca.Replicas[0]]++
		}
	}
	// Make sure every broker has an entry in leaderCount.
	for _, id := range b.state.BrokerIDs {
		if _, ok := leaderCount[id]; !ok {
			leaderCount[id] = 0
		}
	}

	for total < desiredTotal {
		// Pick a preferred leader for the new partition.
		preferred := pickLeader(b.state.BrokerIDs, leaderCount, b.desired.TargetLeaders)

		// Materialize replicas and commit the create.
		reps := sel.ChooseReplicas(preferred, b.desired.RF)
		b.CommitCreate(reps)

		leaderCount[preferred]++
		total++
	}
}

// pickLeader returns the broker to prefer as the new partition's leader.
//
// If any brokers are still below their soft target, it returns the one with the
// smallest positive remaining gap (# of missing leaderships for partitions).
// Otherwise, it returns the least-loaded leader.
func pickLeader(brokerIDs []int32, leaderCount map[int32]int, target map[int32]int) int32 {
	var (
		chosen  int32
		bestGap int // smallest positive gap seen so far
		found   bool
	)

	for _, id := range brokerIDs {
		gap := target[id] - leaderCount[id] // how many leaders this broker still “should” get
		if gap <= 0 {
			continue
		}
		if !found || gap < bestGap {
			chosen = id
			bestGap = gap
			found = true
		}
	}
	if found {
		return chosen
	}
	// Everyone at/above target: fall back to least leaders.
	return leastLoadedLeader(leaderCount, brokerIDs)
}

// -----------------------------------------------------------------------------
// State & Desired
// -----------------------------------------------------------------------------

// Broker is an immutable snapshot of a broker’s ID and rack label at plan time.
// An empty rack value means the broker did not advertise one.
//
// We never mutate this during planning; it mirrors the Metadata response.
type Broker struct {
	ID   int32
	Rack string // empty string if unknown
}

// Partition is an immutable snapshot of a partition's current replica list.
//
// The order of Replicas matters: index 0 is the *preferred leader* for this
// partition, i.e., where the controller will attempt to place leadership after
// changes. The Leader field contains the *actual current leader* as reported
// by Kafka metadata, which may differ from the preferred leader (replicas[0]).
//
// We do not mutate these structs; instead we work with a predictive "view"
// inside PlanBuilder (below).
type Partition struct {
	ID       int32
	Leader   int32   // actual current leader from metadata
	Replicas []int32 // order matters; index 0 is preferred leader
}

// ClusterState is a convenience wrapper around the metadata we actually need in
// this planner. It is deliberately small to keep the code readable.
//
// - TopicName: the probe topic name (for logging and request building)
// - Brokers: map of broker id -> Broker
// - BrokerIDs: sorted slice of broker IDs for deterministic iteration
// - ByRack: rack -> broker IDs, to reason about rack diversity
// - NumRacks: number of distinct racks (empty rack counts as one)
// - Partitions: map of partition id -> Partition snapshot
//
// None of the members are mutated after construction.
type ClusterState struct {
	TopicName  string
	Brokers    map[int32]Broker
	BrokerIDs  []int32 // sorted
	ByRack     map[string][]int32
	NumRacks   int
	Partitions map[int32]Partition
}

// BuildState converts Metadata response to ClusterState and copies slices so
// planning cannot accidentally mutate the input.
func BuildState(meta *kmsg.MetadataResponse) ClusterState {
	brokers := make(map[int32]Broker, len(meta.Brokers))
	byRack := make(map[string][]int32)
	var ids []int32
	for _, b := range meta.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		brokers[b.NodeID] = Broker{ID: b.NodeID, Rack: rack}
		byRack[rack] = append(byRack[rack], b.NodeID)
		ids = append(ids, b.NodeID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	parts := make(map[int32]Partition)
	for _, p := range meta.Topics[0].Partitions {
		rep := append([]int32(nil), p.Replicas...)
		parts[p.Partition] = Partition{ID: p.Partition, Leader: p.Leader, Replicas: rep}
	}

	return ClusterState{
		TopicName:  pointerStrToStr(meta.Topics[0].Topic),
		Brokers:    brokers,
		BrokerIDs:  ids,
		ByRack:     byRack,
		NumRacks:   len(byRack),
		Partitions: parts,
	}
}

type Desired struct {
	RF                int
	DesiredPartitions int
	TargetLeaders     map[int32]int // per broker (even split)
}

// ComputeDesired derives the minimal partition count and a soft target for
// preferred leader distribution. We require at least one partition per broker so
// everyone can lead, and we honor PartitionsPerBroker as an additional lower
// bound.
func ComputeDesired(state ClusterState, cfg EndToEndTopicConfig) Desired {
	perBroker := cfg.PartitionsPerBroker
	if perBroker < 1 {
		perBroker = 1
	}
	cur := len(state.Partitions)
	desiredPartitions := max(cur, perBroker*len(state.BrokerIDs))
	target := evenSplit(desiredPartitions, state.BrokerIDs)
	return Desired{RF: cfg.ReplicationFactor, DesiredPartitions: desiredPartitions, TargetLeaders: target}
}

// -----------------------------------------------------------------------------
// Replica selection (rack-aware) & load tracking
// -----------------------------------------------------------------------------

// Load captures, per broker, how many times it appears as a replica and as a
// preferred leader across the predictive view of the topic. We use this to
// bias selection towards less-loaded brokers to avoid hot spots.
//
// Note: this is a transient view local to a single planning run. Nothing here
// is persisted and no attempt is made to perfectly balance replicas across the
// cluster—only to avoid obviously uneven choices.
type Load struct {
	Replicas int
	Leaders  int
}

// LoadTracker is a tiny helper holding a per-broker Load map with convenience
// methods for reading and incrementing counts while the plan is being
// constructed.
//
// Why not compute loads on the fly? We do that initially (from current
// assignments) and then update incrementally while choosing replicas for new or
// changed partitions to keep subsequent choices informed by earlier ones.
// Keeping it explicit in a struct makes the intent obvious and testing easier.
type LoadTracker struct {
	l map[int32]Load
}

// NewLoadTracker builds initial loads from the current assignments in the
// metadata (before any staged changes). The caller updates loads as it makes
// predictive choices so the next decision can see the latest picture.
func NewLoadTracker(state ClusterState) *LoadTracker {
	l := make(map[int32]Load, len(state.BrokerIDs))
	for _, id := range state.BrokerIDs {
		l[id] = Load{}
	}
	for _, p := range state.Partitions {
		for _, r := range p.Replicas {
			ld := l[r]
			ld.Replicas++
			l[r] = ld
		}
		// Count the actual current leader separately
		if p.Leader != -1 { // -1 indicates no leader (error state)
			ld := l[p.Leader]
			ld.Leaders++
			l[p.Leader] = ld
		}
	}
	return &LoadTracker{l: l}
}

// AddReplica increments replica and (optionally) leader counts for a broker in
// the predictive view. Call this after the planner decides to place a replica
// (e.g., in ChooseReplicas).
func (t *LoadTracker) AddReplica(id int32, leader bool) {
	ld := t.l[id]
	ld.Replicas++
	if leader {
		ld.Leaders++
	}
	t.l[id] = ld
}

// Load returns the current transient load counters for a broker.
func (t *LoadTracker) Load(id int32) Load { return t.l[id] }

// ReplicaSelector abstracts the heuristic used to pick a concrete replica set
// for a given (preferred) leader and RF. Keeping this as an interface makes it
// trivial to swap strategy in tests if you’d like to assert specific behaviors.
// The production strategy we use is RackAwareSelector.
type ReplicaSelector interface {
	ChooseReplicas(preferredLeader int32, rf int) []int32
}

// RackAwareSelector is a simple greedy strategy that tries to:
//  1. Always include the requested preferred leader at index 0.
//  2. Maximize rack diversity by preferring brokers on new racks first.
//  3. Among candidates on equally novel racks, prefer lower replica load.
//  4. Use broker ID as a final tiebreaker for determinism.
//
// After selecting, we update the transient LoadTracker so subsequent decisions
// are informed by this choice.
//
// This is intentionally not perfect or global-optimal—just a pragmatic heuristic
// that produces good, stable results for the probe topic.
type RackAwareSelector struct {
	state ClusterState
	loads *LoadTracker
}

// NewRackAwareSelector constructs the default selection strategy.
func NewRackAwareSelector(state ClusterState, loads *LoadTracker) *RackAwareSelector {
	return &RackAwareSelector{state: state, loads: loads}
}

// ChooseReplicas returns an ordered replica list of length rf where index 0 is
// the preferred leader. Candidates are scored by new rack first, then lower
// load, then lower broker ID.
func (s *RackAwareSelector) ChooseReplicas(preferredLeader int32, rf int) []int32 {
	rf = min(rf, len(s.state.BrokerIDs))
	res := make([]int32, 0, rf)
	res = append(res, preferredLeader)
	seen := map[int32]struct{}{preferredLeader: {}}
	usedRack := map[string]struct{}{s.state.Brokers[preferredLeader].Rack: {}}
	type cand struct {
		id   int32
		rack string
		load int
	}
	build := func() []cand {
		out := make([]cand, 0, len(s.state.BrokerIDs))
		for _, id := range s.state.BrokerIDs {
			if _, ok := seen[id]; ok {
				continue
			}
			out = append(out, cand{id: id, rack: s.state.Brokers[id].Rack, load: s.loads.Load(id).Replicas})
		}
		sort.Slice(out, func(i, j int) bool {
			_, iu := usedRack[out[i].rack]
			_, ju := usedRack[out[j].rack]
			if iu != ju {
				return !iu && ju
			}
			if out[i].load != out[j].load {
				return out[i].load < out[j].load
			}
			return out[i].id < out[j].id
		})
		return out
	}
	for len(res) < rf {
		cands := build()
		if len(cands) == 0 {
			break
		}
		c := cands[0]
		res = append(res, c.id)
		seen[c.id] = struct{}{}
		usedRack[c.rack] = struct{}{}
	}
	// update transient loads
	for i, id := range res {
		s.loads.AddReplica(id, i == 0)
	}
	return res
}

// -----------------------------------------------------------------------------
// Plan -> Kafka requests
// -----------------------------------------------------------------------------

// ToRequests converts a Plan to Kafka admin requests. Either result may be nil
// if the plan contains no operations of that type.
func (p *Plan) ToRequests(topic string) (*kmsg.AlterPartitionAssignmentsRequest, *kmsg.CreatePartitionsRequest) {
	var alter *kmsg.AlterPartitionAssignmentsRequest
	var create *kmsg.CreatePartitionsRequest

	if len(p.Reassignments) > 0 {
		r := kmsg.NewAlterPartitionAssignmentsRequest()
		t := kmsg.NewAlterPartitionAssignmentsRequestTopic()
		t.Topic = topic
		for _, ra := range p.Reassignments {
			pr := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
			pr.Partition = ra.Partition
			pr.Replicas = append([]int32(nil), ra.Replicas...)
			t.Partitions = append(t.Partitions, pr)
		}
		r.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{t}
		alter = &r
	}

	if len(p.CreateAssignments) > 0 {
		r := kmsg.NewCreatePartitionsRequest()
		t := kmsg.NewCreatePartitionsRequestTopic()
		t.Topic = topic
		t.Count = int32(p.FinalPartitionCount)
		for _, ca := range p.CreateAssignments {
			ta := kmsg.NewCreatePartitionsRequestTopicAssignment()
			ta.Replicas = append([]int32(nil), ca.Replicas...)
			t.Assignment = append(t.Assignment, ta)
		}
		r.Topics = []kmsg.CreatePartitionsRequestTopic{t}
		create = &r
	}
	return alter, create
}

// -----------------------------------------------------------------------------
// Utils
// -----------------------------------------------------------------------------

// indexLeaders builds broker -> list of partition IDs it currently leads (from view).
func indexLeaders(brokerIDs []int32, view map[int32][]int32) map[int32][]int32 {
	m := make(map[int32][]int32, len(brokerIDs))
	for _, id := range brokerIDs {
		m[id] = nil
	}
	for pid, reps := range view {
		if len(reps) > 0 {
			m[reps[0]] = append(m[reps[0]], pid)
		}
	}
	return m
}

// brokersMissingLeadership returns brokers that lead zero partitions.
func brokersMissingLeadership(brokerIDs []int32, leadersByBroker map[int32][]int32) []int32 {
	var out []int32
	for _, id := range brokerIDs {
		if len(leadersByBroker[id]) == 0 {
			out = append(out, id)
		}
	}
	return out
}

// donorBrokers returns brokers that currently lead more than their soft target,
// sorted by largest surplus first; ties broken by broker ID ascending.
func donorBrokers(brokerIDs []int32, leadersByBroker map[int32][]int32, target map[int32]int) []int32 {
	var donors []int32
	for _, id := range brokerIDs {
		if len(leadersByBroker[id]) > target[id] {
			donors = append(donors, id)
		}
	}
	sort.Slice(donors, func(i, j int) bool {
		surplusI := len(leadersByBroker[donors[i]]) - target[donors[i]]
		surplusJ := len(leadersByBroker[donors[j]]) - target[donors[j]]
		if surplusI != surplusJ {
			return surplusI > surplusJ
		}
		return donors[i] < donors[j]
	})
	return donors
}

// violatesRackDiversity returns true if a partition’s replicas do not use as
// many unique racks as they could (bounded by min(len(reps), numRacks)).
func violatesRackDiversity(reps []int32, numRacks int, brokers map[int32]Broker) bool {
	if len(reps) <= 1 {
		return false
	}
	seen := map[string]struct{}{}
	for _, r := range reps {
		seen[brokers[r].Rack] = struct{}{}
	}
	maxUnique := min(len(reps), numRacks)
	return len(seen) < maxUnique
}

// victimIndex returns the index of a replica that sits on a rack appearing more
// than once within the replica set (i.e., a duplicate-rack candidate). This is
// used when we need to swap in a new broker to keep/restore diversity. If none
// exists, we fall back to the last replica (a stable, simple choice that avoids
// touching the leader at index 0).
func victimIndex(reps []int32, brokers map[int32]Broker) int {
	rc := map[string]int{}
	for _, r := range reps {
		rc[brokers[r].Rack]++
	}
	for i, r := range reps {
		if rc[brokers[r].Rack] > 1 {
			return i
		}
	}
	return len(reps) - 1 // fallback: last (never the leader)
}

// remove removes v from a slice without preserving order. Used for maintaining
// the leaders map in ensureLeaderCoverage.
func remove(xs []int32, v int32) []int32 {
	out := xs[:0]
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

// putFirst moves id to index 0 while preserving the relative order of the
// remaining elements. This models changing the preferred leader.
func putFirst(reps []int32, id int32) []int32 {
	out := make([]int32, 0, len(reps))
	out = append(out, id)
	for _, r := range reps {
		if r != id {
			out = append(out, r)
		}
	}
	return out
}

// contains reports whether v is present in xs.
func contains(xs []int32, v int32) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// evenSplit returns a soft target leader count per broker such that totals sum
// to n. The remainder (+1) is assigned to the lowest broker IDs for stability.
func evenSplit(n int, ids []int32) map[int32]int {
	m := make(map[int32]int, len(ids))
	if len(ids) == 0 {
		return m
	}
	base := n / len(ids)
	rem := n % len(ids)
	for i, id := range ids {
		m[id] = base
		if i < rem {
			m[id]++
		}
	}
	return m
}

// leastLoadedLeader returns the broker with the smallest number of preferred
// leader assignments (ties broken by smaller broker ID).
func leastLoadedLeader(leaders map[int32]int, ids []int32) int32 {
	best, bestCnt := ids[0], 1<<30
	for _, id := range ids {
		if leaders[id] < bestCnt || (leaders[id] == bestCnt && id < best) {
			bestCnt, best = leaders[id], id
		}
	}
	return best
}

func equalInt32s(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
