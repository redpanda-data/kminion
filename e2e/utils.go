package e2e

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// createHistogramBuckets creates the buckets for the histogram based on the number of desired buckets (10) and the
// upper bucket size.
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

// logCommitErrors logs all errors in commit response and returns a well formatted error code if there was one
func (s *Service) logCommitErrors(r *kmsg.OffsetCommitResponse, err error) string {
	if err != nil {
		if err == context.DeadlineExceeded {
			s.logger.Warn("offset commit failed because SLA has been exceeded")
			return "OFFSET_COMMIT_SLA_EXCEEDED"
		}

		s.logger.Warn("offset commit failed", zap.Error(err))
		return "RESPONSE_ERROR"
	}

	lastErrCode := ""
	for _, t := range r.Topics {
		for _, p := range t.Partitions {
			typedErr := kerr.TypedErrorForCode(p.ErrorCode)
			if typedErr == nil {
				continue
			}

			s.logger.Warn("error committing partition offset",
				zap.String("topic", t.Topic),
				zap.Int32("partition_id", p.Partition),
				zap.Error(typedErr),
			)
			lastErrCode = typedErr.Message
		}
	}

	return lastErrCode
}

// brokerMetadataByBrokerID returns a map of all broker metadata keyed by their BrokerID
//
//nolint:unused
func brokerMetadataByBrokerID(meta []kmsg.MetadataResponseBroker) map[int32]kmsg.MetadataResponseBroker {
	res := make(map[int32]kmsg.MetadataResponseBroker)
	for _, broker := range meta {
		res[broker.NodeID] = broker
	}
	return res
}

// brokerMetadataByRackID returns a map of all broker metadata keyed by their Rack identifier
//
//nolint:unused
func brokerMetadataByRackID(meta []kmsg.MetadataResponseBroker) map[string][]kmsg.MetadataResponseBroker {
	res := make(map[string][]kmsg.MetadataResponseBroker)
	for _, broker := range meta {
		rackID := ""
		if broker.Rack != nil {
			rackID = *broker.Rack
		}
		res[rackID] = append(res[rackID], broker)
	}
	return res
}

func pointerStrToStr(str *string) string {
	if str == nil {
		return ""
	}
	return *str
}

//nolint:unused
func safeUnwrap(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func isInArray(num int16, arr []int16) bool {
	for _, n := range arr {
		if num == n {
			return true
		}
	}
	return false
}
