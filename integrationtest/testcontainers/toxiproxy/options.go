package toxiproxy

import (
	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	exposedPorts []string
}

func defaultOptions() options {
	return options{}
}

// Compiler check to ensure that Option implements the testcontainers.ContainerCustomizer interface.
var _ testcontainers.ContainerCustomizer = (*Option)(nil)

// Option is an option for the Redpanda container.
type Option func(*options)

// Customize is a NOOP. It's defined to satisfy the testcontainers.ContainerCustomizer interface.
func (o Option) Customize(*testcontainers.GenericContainerRequest) {
	// NOOP to satisfy interface.
}

// WithExposedPorts allows to expose additional ports.
func WithExposedPorts(ports ...string) Option {
	return func(o *options) {
		o.exposedPorts = append(o.exposedPorts, ports...)
	}
}
