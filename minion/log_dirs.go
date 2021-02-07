package minion

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type LogDirResponseShard struct {
	Err     error
	Broker  kgo.BrokerMetadata
	LogDirs *kmsg.DescribeLogDirsResponse
}

func (s *Service) DescribeLogDirs(ctx context.Context) []LogDirResponseShard {
	req := kmsg.NewDescribeLogDirsRequest()
	req.Topics = nil // Describe all topics
	responses := s.kafkaSvc.Client.RequestSharded(ctx, &req)

	res := make([]LogDirResponseShard, len(responses))
	for i, responseShard := range responses {
		logDirs, ok := responseShard.Resp.(*kmsg.DescribeLogDirsResponse)
		if !ok {
			logDirs = &kmsg.DescribeLogDirsResponse{}
		}

		res[i] = LogDirResponseShard{
			Err:     responseShard.Err,
			Broker:  responseShard.Meta,
			LogDirs: logDirs,
		}
	}

	return res
}
