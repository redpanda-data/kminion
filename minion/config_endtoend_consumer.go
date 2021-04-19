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
	latency, _ := time.ParseDuration("20s")
	c.LatencySla = latency
}

func (c *EndToEndConsumerConfig) Validate() error {

	switch c.RebalancingProtocol {
	case RoundRobin, Range, Sticky, CooperativeSticky:
	default:
		return fmt.Errorf("given RebalancingProtocol '%v' is invalid", c.RebalancingProtocol)
	}

	return nil
}
