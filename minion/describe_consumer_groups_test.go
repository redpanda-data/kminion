package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPartitionGroupIDsByType(t *testing.T) {
	tests := []struct {
		name             string
		groups           []kmsg.ListGroupsResponseGroup
		expectedClassic  []string
		expectedConsumer []string
	}{
		{
			name: "splits groups by their reported type",
			groups: []kmsg.ListGroupsResponseGroup{
				{Group: "classic-group", GroupType: "classic"},
				{Group: "kip848-group", GroupType: "consumer"},
			},
			expectedClassic:  []string{"classic-group"},
			expectedConsumer: []string{"kip848-group"},
		},
		{
			name: "treats groups without a reported type as classic",
			groups: []kmsg.ListGroupsResponseGroup{
				{Group: "old-broker-group"},
			},
			expectedClassic:  []string{"old-broker-group"},
			expectedConsumer: []string{},
		},
		{
			name: "matches the group type case insensitively",
			groups: []kmsg.ListGroupsResponseGroup{
				{Group: "kip848-group", GroupType: "CONSUMER"},
			},
			expectedClassic:  []string{},
			expectedConsumer: []string{"kip848-group"},
		},
		{
			name: "treats unknown group types as classic",
			groups: []kmsg.ListGroupsResponseGroup{
				{Group: "share-group", GroupType: "share"},
			},
			expectedClassic:  []string{"share-group"},
			expectedConsumer: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listRes := kmsg.NewPtrListGroupsResponse()
			listRes.Groups = test.groups

			classicGroupIDs, consumerGroupIDs := partitionGroupIDsByType(listRes)

			assert.Equal(t, test.expectedClassic, classicGroupIDs)
			assert.Equal(t, test.expectedConsumer, consumerGroupIDs)
		})
	}
}

func TestConsumerGroupDescribeToDescribeGroups(t *testing.T) {
	t.Run("converts a group along with its members", func(t *testing.T) {
		instanceID := "instance-id"

		member := kmsg.NewConsumerGroupDescribeResponseGroupMember()
		member.MemberID = "member-id"
		member.InstanceID = &instanceID
		member.ClientID = "client-id"
		member.ClientHost = "client-host"
		member.Assignment.TopicPartitions = []kmsg.AssignmentTopicPartition{
			{Topic: "topic-a", Partitions: []int32{0, 1}},
			{Topic: "topic-b", Partitions: []int32{2}},
		}

		group := kmsg.NewConsumerGroupDescribeResponseGroup()
		group.Group = "kip848-group"
		group.State = "Stable"
		group.AssignorName = "uniform"
		group.Members = []kmsg.ConsumerGroupDescribeResponseGroupMember{member}

		res := kmsg.NewPtrConsumerGroupDescribeResponse()
		res.Groups = []kmsg.ConsumerGroupDescribeResponseGroup{group}

		converted := consumerGroupDescribeToDescribeGroups(res)

		require.Len(t, converted.Groups, 1)
		convertedGroup := converted.Groups[0]
		assert.Equal(t, "kip848-group", convertedGroup.Group)
		assert.Equal(t, "Stable", convertedGroup.State)
		// The consumer rebalance protocol reports no protocol type, but the exported labels must stay
		// consistent with what the classic protocol reports for consumer groups.
		assert.Equal(t, "consumer", convertedGroup.ProtocolType)
		assert.Equal(t, "uniform", convertedGroup.Protocol)

		require.Len(t, convertedGroup.Members, 1)
		convertedMember := convertedGroup.Members[0]
		assert.Equal(t, "member-id", convertedMember.MemberID)
		assert.Equal(t, &instanceID, convertedMember.InstanceID)
		assert.Equal(t, "client-id", convertedMember.ClientID)
		assert.Equal(t, "client-host", convertedMember.ClientHost)

		// The assignment must be readable with the classic member assignment format, because that is how
		// the exporter decodes assignments for every group.
		assignment := kmsg.NewConsumerMemberAssignment()
		require.NoError(t, assignment.ReadFrom(convertedMember.MemberAssignment))
		require.Len(t, assignment.Topics, 2)
		assert.Equal(t, "topic-a", assignment.Topics[0].Topic)
		assert.Equal(t, []int32{0, 1}, assignment.Topics[0].Partitions)
		assert.Equal(t, "topic-b", assignment.Topics[1].Topic)
		assert.Equal(t, []int32{2}, assignment.Topics[1].Partitions)
	})

	t.Run("keeps the error code of a group that could not be described", func(t *testing.T) {
		group := kmsg.NewConsumerGroupDescribeResponseGroup()
		group.Group = "missing-group"
		group.ErrorCode = 69 // GROUP_ID_NOT_FOUND

		res := kmsg.NewPtrConsumerGroupDescribeResponse()
		res.Groups = []kmsg.ConsumerGroupDescribeResponseGroup{group}

		converted := consumerGroupDescribeToDescribeGroups(res)

		require.Len(t, converted.Groups, 1)
		assert.Equal(t, int16(69), converted.Groups[0].ErrorCode)
		assert.Empty(t, converted.Groups[0].Members)
	})

	t.Run("reports a member without an assignment as unassigned", func(t *testing.T) {
		member := kmsg.NewConsumerGroupDescribeResponseGroupMember()
		member.MemberID = "member-id"

		group := kmsg.NewConsumerGroupDescribeResponseGroup()
		group.Group = "kip848-group"
		group.Members = []kmsg.ConsumerGroupDescribeResponseGroupMember{member}

		res := kmsg.NewPtrConsumerGroupDescribeResponse()
		res.Groups = []kmsg.ConsumerGroupDescribeResponseGroup{group}

		converted := consumerGroupDescribeToDescribeGroups(res)

		require.Len(t, converted.Groups, 1)
		require.Len(t, converted.Groups[0].Members, 1)
		assert.Empty(t, converted.Groups[0].Members[0].MemberAssignment)
	})
}
