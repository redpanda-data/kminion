package toxiproxy

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Container represents the Toxiproxy container type used in the module.
type Container struct {
	testcontainers.Container
}

const (
	defaultHTTPPort = "8474/tcp"
)

// RunContainer creates an instance of the Redpanda container type.
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	tmpDir, err := os.MkdirTemp("", "redpanda")
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Create container request.
	// Some (e.g. Image) may be overridden by providing an option argument to this function.
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/shopify/toxiproxy:2.7.0",
			ExposedPorts: []string{defaultHTTPPort, "8666/tcp"},
			WaitingFor:   wait.ForHTTP("/version").WithPort(defaultHTTPPort),
		},
		Started: true,
	}

	// 2. Gather all config options (defaults and then apply provided options)
	settings := defaultOptions()
	for _, opt := range opts {
		if apply, ok := opt.(Option); ok {
			apply(&settings)
		}
		opt.Customize(&req)
	}

	req.ContainerRequest.ExposedPorts = append(req.ContainerRequest.ExposedPorts, settings.exposedPorts...)

	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return nil, err
	}

	err = wait.ForAll(
		wait.ForHTTP("/version").WithPort("8474/tcp")).
		WaitUntilReady(ctx, container)

	if err != nil {
		return nil, fmt.Errorf("failed to wait for Toxiproxy readiness: %w", err)
	}

	return &Container{Container: container}, nil
}

// HTTPAddress returns the server address that can be used
// by Toxiproxy clients to configure toxics. It is returned
// in the following format: "http://ip:port".
func (c *Container) HTTPAddress(ctx context.Context) (string, error) {
	return c.PortEndpoint(ctx, defaultHTTPPort, "http")
}

// MappedHostPort returns the host and port separately for the request nat.Port
func (c *Container) MappedHostPort(ctx context.Context, containerPort nat.Port) (string, string, error) {
	hostPort, err := c.PortEndpoint(ctx, containerPort, "")
	if err != nil {
		return "", "", err
	}
	return net.SplitHostPort(hostPort)
}
