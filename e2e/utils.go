package e2e

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// create histogram buckets for metrics reported by 'end-to-end'
// todo:
/*
- custom, much simpler, exponential buckets
  we know:
  	- we want to go from 5ms to 'max'
	- we want to double each time
	- doubling 5ms might not get us to 'max' exactly
  questions:
	- can we slightly adjust the factor so we hit 'max' exactly?
	- or can we adjust 'max'?
		(and if so, better to overshoot or undershoot?)
	- or should we just set the last bucket to 'max' exactly?
*/
func createHistogramBuckets(maxLatency time.Duration) []float64 {
	// Since this is an exponential bucket we need to take Log base2 or binary as the upper bound
	// Divide by 10 for the argument because the base is counted as 20ms and we want to normalize it as base 2 instead of 20
	// +2 because it starts at 5ms or 0.005 sec, to account 5ms and 10ms before it goes to the base which in this case is 0.02 sec or 20ms
	// and another +1 to account for decimal points on int parsing
	latencyCount := math.Logb(float64(maxLatency.Milliseconds() / 10))
	count := int(latencyCount) + 3
	bucket := prometheus.ExponentialBuckets(0.005, 2, count)

	return bucket
}

func containsStr(ar []string, x string) (bool, int) {
	for i, item := range ar {
		if item == x {
			return true, i
		}
	}
	return false, -1
}
