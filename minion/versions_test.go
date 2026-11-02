package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRedpandaCluster(t *testing.T) {
	ptr := func(s string) *string { return &s }
	tt := []struct {
		name      string
		clusterID *string
		want      bool
	}{
		{name: "redpanda", clusterID: ptr("redpanda.1362d2b8-d826-40b8-b481-a0885190e328"), want: true},
		{name: "redpanda initializing", clusterID: ptr("redpanda.initializing"), want: true},
		{name: "kafka", clusterID: ptr("UYZJg8bhT_6SxhsdaQZEQ"), want: false},
		{name: "no cluster id", clusterID: nil, want: false},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isRedpandaCluster(tc.clusterID))
		})
	}
}
