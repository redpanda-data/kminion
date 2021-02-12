package minion

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kmsg"
	"time"
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
	req := kmsg.NewMetadataRequest()
	req.Topics = nil

	res, err := req.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to request metadata: %w", err)
	}

	return res, nil
}
