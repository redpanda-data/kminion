package minion

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// GetClusterVersion returns the guessed Kafka version of the cluster, or "redpanda" for a
// Redpanda cluster. Redpanda does not expose its version via the Kafka API, and a Kafka version
// guessed from its API versions is misleading.
func (s *Service) GetClusterVersion(ctx context.Context) (string, error) {
	metadata, err := s.GetMetadataCached(ctx)
	if err != nil {
		return "", err
	}
	if isRedpandaCluster(metadata.ClusterID) {
		return "redpanda", nil
	}

	res, err := s.GetAPIVersions(ctx)
	if err != nil {
		return "", err
	}

	versions := kversion.FromApiVersionsResponse(res)
	return versions.VersionGuess(), nil
}

// isRedpandaCluster reports whether the cluster ID belongs to Redpanda. Redpanda always
// prefixes its cluster ID with "redpanda.", also while the ID is not initialized yet.
func isRedpandaCluster(clusterID *string) bool {
	return clusterID != nil && strings.HasPrefix(*clusterID, "redpanda.")
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
