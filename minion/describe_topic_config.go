package minion

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Service) GetTopicConfigs(ctx context.Context) (*kmsg.DescribeConfigsResponse, error) {
	metadata, err := s.GetMetadataCached(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata")
	}

	req := kmsg.NewDescribeConfigsRequest()

	for _, topic := range metadata.Topics {
		resourceReq := kmsg.NewDescribeConfigsRequestResource()
		resourceReq.ResourceType = kmsg.ConfigResourceTypeTopic
		resourceReq.ResourceName = *topic.Topic
		req.Resources = append(req.Resources, resourceReq)
	}

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to request metadata: %w", err)
	}

	return res, nil
}
