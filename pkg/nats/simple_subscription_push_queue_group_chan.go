package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

type simplePushQueueGroupChanSubscription struct {
	natsSubs *nats.Subscription
	natsConn *nats.Conn

	subjectName string
	groupName   string

	autoReSubscribe        bool
	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration

	msgChannel chan *nats.Msg

	logger *zap.Logger
}

func (s *simplePushQueueGroupChanSubscription) OnClosed(conn *nats.Conn) error {
	s.natsSubs = nil
	s.natsConn = nil

	close(s.msgChannel)

	return nil
}

func (s *simplePushQueueGroupChanSubscription) OnReconnect(newConn *nats.Conn) error {
	s.natsConn = newConn

	err := s.tryResubscribe()
	if err != nil {
		return err
	}

	return nil
}

func (s *simplePushQueueGroupChanSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *simplePushQueueGroupChanSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *simplePushQueueGroupChanSubscription) Init(ctx context.Context) error {
	return nil
}

func (s *simplePushQueueGroupChanSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.natsConn.ChanQueueSubscribe(s.subjectName, s.groupName, s.msgChannel)
	if err != nil {
		return err
	}

	s.natsSubs = subs

	return nil
}

func (s *simplePushQueueGroupChanSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

	return nil
}

func (s *simplePushQueueGroupChanSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.natsConn.ChanQueueSubscribe(s.subjectName, s.groupName, s.msgChannel)
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

func newSimplePushQueueGroupSubscriptionService(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	msgChannel chan *nats.Msg,
) *simplePushQueueGroupChanSubscription {
	l := logger.Named("subscription")

	return &simplePushQueueGroupChanSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName: consumerCfg.GetSubjectName(),
		groupName:   consumerCfg.GetQueueGroupName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),

		msgChannel: msgChannel,
		logger:     l,
	}
}
