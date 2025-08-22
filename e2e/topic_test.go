package e2e

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func TestCalculateAppropriateReplicas(t *testing.T) {
	tt := []struct {
		TestName          string
		Brokers           []kmsg.MetadataResponseBroker
		ReplicationFactor int
		LeaderBroker      kmsg.MetadataResponseBroker

		// Some cases may have more than one possible solution, each entry in the outer array covers one allowed
		// solution. The compared int32 array order does not matter, except for the very first item as this indicates
		// the preferred leader. For example if you use {2, 0, 1} as expected result this would also be valid for
		// the actual result {2, 1, 0} but not for {1, 2, 0} - because '2' must be the first int32.
		ExpectedResults [][]int32
	}{
		{
			TestName: "3 Brokers, no rack, RF = 3",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 0, Rack: nil},
				{NodeID: 1, Rack: nil},
				{NodeID: 2, Rack: nil},
			},
			ReplicationFactor: 3,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 2, Rack: nil},
			ExpectedResults:   [][]int32{{2, 0, 1}},
		},

		{
			TestName: "3 Brokers, 3 racks, RF = 3",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 0, Rack: kmsg.StringPtr("a")},
				{NodeID: 1, Rack: kmsg.StringPtr("b")},
				{NodeID: 2, Rack: kmsg.StringPtr("c")},
			},
			ReplicationFactor: 3,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 2, Rack: kmsg.StringPtr("c")},
			ExpectedResults:   [][]int32{{2, 0, 1}},
		},

		{
			TestName: "3 Brokers, 3 racks, RF = 1",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 0, Rack: kmsg.StringPtr("a")},
				{NodeID: 1, Rack: kmsg.StringPtr("b")},
				{NodeID: 2, Rack: kmsg.StringPtr("c")},
			},
			ReplicationFactor: 1,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 1, Rack: kmsg.StringPtr("b")},
			ExpectedResults:   [][]int32{{1}},
		},

		{
			TestName: "3 Brokers, 3 racks, RF = 2",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 0, Rack: kmsg.StringPtr("a")},
				{NodeID: 1, Rack: kmsg.StringPtr("b")},
				{NodeID: 2, Rack: kmsg.StringPtr("c")},
			},
			ReplicationFactor: 2,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 1, Rack: kmsg.StringPtr("b")},
			ExpectedResults:   [][]int32{{1, 0}, {1, 2}},
		},

		{
			TestName: "6 Brokers, 3 racks, RF = 3",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 0, Rack: kmsg.StringPtr("a")},
				{NodeID: 1, Rack: kmsg.StringPtr("b")},
				{NodeID: 2, Rack: kmsg.StringPtr("c")},
				{NodeID: 3, Rack: kmsg.StringPtr("a")},
				{NodeID: 4, Rack: kmsg.StringPtr("b")},
				{NodeID: 5, Rack: kmsg.StringPtr("c")},
			},
			ReplicationFactor: 3,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 4, Rack: kmsg.StringPtr("b")},
			ExpectedResults:   [][]int32{{4, 0, 2}, {4, 0, 5}, {4, 3, 2}, {4, 3, 5}},
		},

		{
			TestName: "4 Brokers, 2 racks, RF = 3",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 0, Rack: kmsg.StringPtr("a")},
				{NodeID: 1, Rack: kmsg.StringPtr("b")},
				{NodeID: 2, Rack: kmsg.StringPtr("a")},
				{NodeID: 3, Rack: kmsg.StringPtr("b")},
			},
			ReplicationFactor: 3,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 0, Rack: kmsg.StringPtr("a")},
			ExpectedResults:   [][]int32{{0, 1, 2}, {0, 1, 3}, {0, 2, 3}},
		},

		{
			TestName: "6 Brokers, 3 racks, RF = 3, lowest node id != 0",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 10, Rack: kmsg.StringPtr("a")},
				{NodeID: 11, Rack: kmsg.StringPtr("b")},
				{NodeID: 12, Rack: kmsg.StringPtr("c")},
				{NodeID: 13, Rack: kmsg.StringPtr("a")},
				{NodeID: 14, Rack: kmsg.StringPtr("b")},
				{NodeID: 15, Rack: kmsg.StringPtr("c")},
			},
			ReplicationFactor: 3,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 11, Rack: kmsg.StringPtr("b")},
			ExpectedResults:   [][]int32{{11, 10, 12}, {11, 12, 13}, {11, 13, 15}},
		},

		{
			TestName: "6 Brokers, 3 racks, RF = 5, lowest node id != 0",
			Brokers: []kmsg.MetadataResponseBroker{
				{NodeID: 10, Rack: kmsg.StringPtr("a")},
				{NodeID: 11, Rack: kmsg.StringPtr("b")},
				{NodeID: 12, Rack: kmsg.StringPtr("c")},
				{NodeID: 13, Rack: kmsg.StringPtr("a")},
				{NodeID: 14, Rack: kmsg.StringPtr("b")},
				{NodeID: 15, Rack: kmsg.StringPtr("c")},
			},
			ReplicationFactor: 5,
			LeaderBroker:      kmsg.MetadataResponseBroker{NodeID: 11, Rack: kmsg.StringPtr("b")},
			ExpectedResults:   [][]int32{{11, 10, 12, 13, 14}, {11, 10, 13, 14, 15}, {11, 12, 13, 14, 15}, {11, 10, 12, 13, 15}, {11, 10, 12, 14, 15}},
		},
	}

	svc := Service{}
	for _, test := range tt {
		meta := kmsg.NewMetadataResponse()
		meta.Brokers = test.Brokers
		replicaIDs := svc.calculateAppropriateReplicas(&meta, test.ReplicationFactor, test.LeaderBroker)

		matchesAtLeastOneExpectedResult := false
		for _, possibleResult := range test.ExpectedResults {
			isValidResult := possibleResult[0] == replicaIDs[0] && doElementsMatch(possibleResult, replicaIDs)
			if isValidResult {
				matchesAtLeastOneExpectedResult = true
				break
			}
		}
		if !matchesAtLeastOneExpectedResult {
			// Use first elementsmatch to print some valid result along with the actual results.
			assert.ElementsMatch(t, test.ExpectedResults[0], replicaIDs, test.TestName)
		}
	}
}

func TestCalculatePartitionReassignments(t *testing.T) {
	tt := []struct {
		TestName             string
		PartitionAssignments []int32

		ExpectedReassignments *map[int32]int32
		ExpectedAllocations   *[]int32
	}{
		{
			TestName:             "there are partitions to reassign",
			PartitionAssignments: []int32{0, 0, 1},
			ExpectedReassignments: &map[int32]int32{
				0: 2, // reassign one partitions from broker 0 to 2
			},
		},
		{
			TestName:             "no partitions to reassign",
			PartitionAssignments: []int32{0, 1}, // only 2 partitions, need one more to cover all the brokers
			ExpectedAllocations:  &[]int32{2},   // allocate one partition to broker 2
		},
	}

	svc := Service{
		logger: zap.NewNop(),
		config: Config{
			TopicManagement: EndToEndTopicConfig{
				ReplicationFactor: 1,
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.TestName, func(t *testing.T) {
			meta := kmsg.NewMetadataResponse()
			meta.Brokers = []kmsg.MetadataResponseBroker{
				{NodeID: 0},
				{NodeID: 1},
				{NodeID: 2},
			}

			var partitions []kmsg.MetadataResponseTopicPartition
			for _, a := range tc.PartitionAssignments {
				partitions = append(partitions, kmsg.MetadataResponseTopicPartition{
					Replicas: []int32{a},
				})
			}

			meta.Topics = []kmsg.MetadataResponseTopic{
				{Partitions: partitions},
			}

			alterReq, createReq, err := svc.calculatePartitionReassignments(&meta)
			require.NoError(t, err)

			if tc.ExpectedReassignments == nil {
				require.Nil(t, alterReq, "expected no reassignments but got some")
			} else {
				require.NotNil(t, alterReq, "expected reassignments, got nothing")

				topic := alterReq.Topics[0]

				actualReassignments := make(map[int32]int32)
				for _, partition := range topic.Partitions {
					actualReassignments[partition.Partition] = partition.Replicas[0]
				}

				assert.Equal(t, *tc.ExpectedReassignments, actualReassignments)
			}

			if tc.ExpectedAllocations == nil {
				require.Nil(t, createReq, "expected no creations but got some")
			} else {
				require.NotNil(t, createReq, "expected creations, got nothing")

				topic := createReq.Topics[0]

				var actualAllocations []int32
				for _, assignment := range topic.Assignment {
					actualAllocations = append(actualAllocations, assignment.Replicas[0])
				}

				assert.ElementsMatch(t, *tc.ExpectedAllocations, actualAllocations)
			}
		})
	}
}

func doElementsMatch(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}

	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	sort.Slice(b, func(i, j int) bool { return a[i] < a[j] })
	for i, num := range a {
		if num != b[i] {
			return false
		}
	}

	return true
}
