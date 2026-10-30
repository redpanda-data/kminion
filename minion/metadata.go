package minion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Service) GetMetadataCached(ctx context.Context) (*kmsg.MetadataResponse, error) {
	reqId := ctx.Value("requestId").(string)
	key := "metadata-" + reqId

	if cachedRes, exists := s.getCachedItem(key); exists {
		return cachedRes.(*kmsg.MetadataResponse), nil
	}

	res, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		metadata, err := s.GetMetadata(ctx)
		if err != nil {
			return nil, err
		}

		s.setCachedItem(key, metadata, 120*time.Second)

		return metadata, nil
	})
	if err != nil {
		return nil, err
	}

	return res.(*kmsg.MetadataResponse), nil
}

func (s *Service) GetMetadata(ctx context.Context) (*kmsg.MetadataResponse, error) {
	req := s.metadataRequest()

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to request metadata: %w", err)
	}

	return res, nil
}

// metadataRequest asks for every topic only when topic collection is enabled: a
// nil topic list means all topics, an empty one means none. Either way the
// response still carries the brokers, cluster ID and controller ID that the
// cluster and broker collectors read.
func (s *Service) metadataRequest() kmsg.MetadataRequest {
	req := kmsg.NewMetadataRequest()
	if s.Cfg.Topics.Enabled {
		req.Topics = nil
	} else {
		req.Topics = []kmsg.MetadataRequestTopic{}
	}

	return req
}
