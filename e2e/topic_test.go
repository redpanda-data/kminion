package e2e

import (
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kmsg"
	"sort"
	"testing"
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
