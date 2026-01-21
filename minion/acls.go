package minion

import (
	"context"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Service) ListAllACLs(ctx context.Context) (*kmsg.DescribeACLsResponse, error) {
	req := kmsg.NewDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.ResourceName = nil
	req.Principal = nil
	req.Host = nil
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny

	res, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return nil, err
	}

	return res, nil
}
