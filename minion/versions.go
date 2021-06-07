package minion

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func (s *Service) GetClusterVersion(ctx context.Context) (string, error) {
	res, err := s.GetAPIVersions(ctx)
	if err != nil {
		return "", err
	}

	versions := kversion.FromApiVersionsResponse(res)
	return versions.VersionGuess(), nil
}

func (s *Service) GetAPIVersions(ctx context.Context) (*kmsg.ApiVersionsResponse, error) {
	versionsReq := kmsg.NewApiVersionsRequest()
	versionsReq.ClientSoftwareName = "kminion"
	versionsReq.ClientSoftwareVersion = "v2"
	res, err := versionsReq.RequestWith(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to request api versions: %w", err)
	}

	err = kerr.ErrorForCode(res.ErrorCode)
	if err != nil {
		return nil, fmt.Errorf("failed to request api versions. Inner kafka error: %w", err)
	}

	return res, nil
}
