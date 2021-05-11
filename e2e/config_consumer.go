package e2e

import (
	"fmt"
	"time"
)

const (
	RoundRobin        string = "roundRobin"
	Range             string = "range"
	Sticky            string = "sticky"
	CooperativeSticky string = "cooperativeSticky"
)

type EndToEndConsumerConfig struct {
	GroupIdPrefix       string `koanf:"groupIdPrefix"`
	RebalancingProtocol string `koanf:"rebalancingProtocol"`

	RoundtripSla time.Duration `koanf:"roundtripSla"`
	CommitSla    time.Duration `koanf:"commitSla"`
}

func (c *EndToEndConsumerConfig) SetDefaults() {
	c.GroupIdPrefix = "kminion-end-to-end"
	c.RebalancingProtocol = "cooperativeSticky"
	c.RoundtripSla = 20 * time.Second
	c.CommitSla = 10 * time.Second // no idea what to use as a good default value
}

func (c *EndToEndConsumerConfig) Validate() error {

	switch c.RebalancingProtocol {
	case RoundRobin, Range, Sticky, CooperativeSticky:
	default:
		return fmt.Errorf("given RebalancingProtocol '%v' is invalid", c.RebalancingProtocol)
	}

	if c.RoundtripSla <= 0 {
		return fmt.Errorf("consumer.roundtripSla must be greater than zero")
	}

	if c.CommitSla <= 0 {
		return fmt.Errorf("consumer.commitSla must be greater than zero")
	}

	return nil
}
