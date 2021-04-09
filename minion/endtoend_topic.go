package minion

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func (s *Service) validateManagementTopic(ctx context.Context) error {

	doesTopicExist, err := s.doesManagementTopicExist(ctx)
	if err != nil {
		return err
	}

	if doesTopicExist {
		// TODO: Do some checking like make sure the topic is replicated to all brokers
		// Check that all brokers has at least one Partition Leader for that topic
	} else {
		s.createManagementTopic(ctx)
		// TODO: Wait until it is propagated and make sure that all brokers has at least one Partition Leader
	}

	return nil
}

func (s *Service) createManagementTopic(ctx context.Context) error {

	s.logger.Info(fmt.Sprintf("creating topic %s for EndToEnd metrics", s.Cfg.EndToEnd.TopicManagement.Name))

	cfg := s.Cfg.EndToEnd.TopicManagement

	topic := kmsg.CreateTopicsRequestTopic{
		Topic:             cfg.Name,
		NumPartitions:     int32(cfg.PartitionsPerBroker),
		ReplicationFactor: int16(cfg.ReplicationFactor),
	}

	req := kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{topic},
	}

	_, err := req.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return fmt.Errorf("failed to request metadata: %w", err)
	}

	return nil
}

func (s *Service) doesManagementTopicExist(ctx context.Context) (bool, error) {

	cfg := s.Cfg.EndToEnd.TopicManagement
	topic := kmsg.MetadataRequestTopic{
		Topic: &cfg.Name,
	}

	req := kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{topic},
	}

	res, err := req.RequestWith(ctx, s.kafkaSvc.Client)
	if err != nil {
		return false, fmt.Errorf("failed to request metadata: %w", err)
	}

	if res.Topics[0].Topic == "" || res.Topics[0].Partitions == nil {
		return false, nil
	}

	return true, nil
}

func (s *Service) initEndToEnd(ctx context.Context) {
	err := s.validateManagementTopic(ctx)
	if err != nil {
		s.logger.Warn("failed to validate management topic for endtoend metrics", zap.Error(err))
		return
	}

	go s.ConsumeFromManagementTopic(ctx)

	t := time.NewTicker(s.Cfg.EndToEnd.ProbeInterval)
	for range t.C {
		s.ProduceToManagementTopic(ctx)
	}
}
