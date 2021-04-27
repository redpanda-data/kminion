package minion

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
	GroupId             string        `koanf:"groupId"`
	LatencySla          time.Duration `koanf:"latencySla"`
	RebalancingProtocol string        `koanf:"rebalancingProtocol"`
}

func (c *EndToEndConsumerConfig) SetDefaults() {
	c.GroupId = "kminion-end-to-end"
	c.RebalancingProtocol = "cooperativeSticky"
	c.LatencySla = 20 * time.Second
}

func (c *EndToEndConsumerConfig) Validate() error {

	switch c.RebalancingProtocol {
	case RoundRobin, Range, Sticky, CooperativeSticky:
	default:
		return fmt.Errorf("given RebalancingProtocol '%v' is invalid", c.RebalancingProtocol)
	}

	// If the timeduration is 0s or 0ms or its variation of zero, it will be parsed as 0
	if c.LatencySla == 0 {
		return fmt.Errorf("failed to validate consumer.latencySla config, the duration can't be zero")
	}

	return nil
}
