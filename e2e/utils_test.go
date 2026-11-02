package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateHistogramBuckets(t *testing.T) {
	tests := []struct {
		name string
		sla  time.Duration
		want []float64
	}{
		{name: "1ms", sla: time.Millisecond, want: []float64{0.005, 0.01, 0.02}},
		{name: "9ms", sla: 9 * time.Millisecond, want: []float64{0.005, 0.01, 0.02}},
		{name: "10ms", sla: 10 * time.Millisecond, want: []float64{0.005, 0.01, 0.02}},
		{name: "5s", sla: 5 * time.Second, want: []float64{0.005, 0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := createHistogramBuckets(tt.sla)
			assert.InDeltaSlice(t, tt.want, buckets, 1e-9)
			assert.GreaterOrEqual(t, buckets[len(buckets)-1], tt.sla.Seconds())
		})
	}
}
