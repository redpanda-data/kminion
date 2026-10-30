package prometheus

import (
	"testing"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDecodeMemberAssignments_InvalidUTF8TopicName(t *testing.T) {
	assignment := kmsg.NewConsumerMemberAssignment()
	topic := kmsg.NewConsumerMemberAssignmentTopic()
	topic.Topic = "\xff"
	topic.Partitions = []int32{0}
	assignment.Topics = append(assignment.Topics, topic)

	member := kmsg.NewDescribeGroupsResponseGroupMember()
	member.MemberAssignment = assignment.AppendTo(nil)

	decoded, err := decodeMemberAssignments("consumer", member)
	require.NoError(t, err)
	require.Len(t, decoded.Topics, 1)

	topicName := decoded.Topics[0].Topic
	require.True(t, utf8.ValidString(topicName))

	desc := prometheus.NewDesc("test_metric", "test", []string{"group_id", "topic_name"}, nil)
	require.NotPanics(t, func() {
		prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1, "group", topicName)
	})
}
