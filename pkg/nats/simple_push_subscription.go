package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

type simplePushChanSubscription struct {
	natsSubs *nats.Subscription
	natsConn *nats.Conn

	subjectName string

	autoReSubscribe        bool
	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration

	handler func(msg *nats.Msg)

	logger *zap.Logger
}

func (s *simplePushChanSubscription) OnReconnect(newConn *nats.Conn) error {
	s.natsConn = newConn

	err := s.tryResubscribe()
	if err != nil {
		return err
	}

	return nil
}

func (s *simplePushChanSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *simplePushChanSubscription) Healthcheck(ctx context.Context) bool {
	if !s.natsConn.IsConnected() {
		s.logger.Warn("consumer lost nats originConn")

		return false
	}

	if !s.natsSubs.IsValid() {
		s.logger.Warn("consumer lost nats subscription")

		return false
	}

	return true
}

func (s *simplePushChanSubscription) Init(ctx context.Context) error {
	return nil
}

func (s *simplePushChanSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.natsConn.Subscribe(s.subjectName, s.handler)
	if err != nil {
		return err
	}

	s.natsSubs = subs

	return nil
}

func (s *simplePushChanSubscription) Shutdown(ctx context.Context) error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

	return nil
}

func (s *simplePushChanSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.natsConn.Subscribe(s.subjectName, s.handler)
		if subsErr != nil {
			s.logger.Warn("unable to re-subscribe", zap.Error(subsErr),
				zap.Uint16(ResubscribeTag, i))

			err = subsErr

			time.Sleep(s.autoReSubscribeTimeout)
			continue
		}

		s.natsSubs = subs

		s.logger.Info("re-subscription success")
		break
	}

	if err != nil {
		return err
	}

	return nil
}

func newSimplePushSubscriptionService(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfig,
	handler func(msg *nats.Msg),
) *simplePushChanSubscription {
	l := logger.Named("subscription")

	return &simplePushChanSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName: consumerCfg.GetSubjectName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),

		handler: handler,

		logger: l,
	}
}
