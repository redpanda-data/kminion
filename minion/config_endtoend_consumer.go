package minion

import "time"

type EndToEndConsumerConfig struct {
	GroupId             string        `koanf:"groupId"`
	LatencySla          time.Duration `koanf:"latencySla"`
	RebalancingProtocol string        `koanf:"rebalancingProtocol"`
}

func (c *EndToEndConsumerConfig) Validate() error {
	return nil
}
