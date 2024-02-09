package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type jsPushQueueGroupChanSubscription struct {
	natsSubs  *nats.Subscription
	natsConn  *nats.Conn
	jsNatsCtx nats.JetStreamContext

	subjectName    string
	queueGroupName string

	autoReSubscribe        bool
	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration
	subscribeNatsOptions   []nats.SubOpt

	msgChannel chan *nats.Msg

	logger *zap.Logger
}

func (s *jsPushQueueGroupChanSubscription) OnReconnect(newConn *nats.Conn) error {
	jsNatsCtx, err := newConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

	s.natsConn = newConn

	err = s.tryResubscribe()
	if err != nil {
		return err
	}

	return nil
}

func (s *jsPushQueueGroupChanSubscription) OnClosed(conn *nats.Conn) error {
	s.natsSubs = nil
	s.jsNatsCtx = nil
	s.natsConn = nil

	return nil
}

func (s *jsPushQueueGroupChanSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPushQueueGroupChanSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *jsPushQueueGroupChanSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPushQueueGroupChanSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.ChanQueueSubscribe(s.subjectName, s.queueGroupName,
		s.msgChannel, s.subscribeNatsOptions...)
	if err != nil {
		return err
	}

	s.natsSubs = subs

	return nil
}

func (s *jsPushQueueGroupChanSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

	return nil
}

func (s *jsPushQueueGroupChanSubscription) onDisconnect(conn *nats.Conn, err error) {

	return
}

func (s *jsPushQueueGroupChanSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil
	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.jsNatsCtx.ChanQueueSubscribe(s.subjectName, s.queueGroupName,
			s.msgChannel, s.subscribeNatsOptions...)
		if subsErr != nil {
			s.logger.Warn("unable to re-subscribe", zap.Error(err),
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

func newJsPushQueueGroupChanSubscriptionService(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	msgChannel chan *nats.Msg,
) *jsPushQueueGroupChanSubscription {
	l := logger.Named("subscription")

	subOptions := []nats.SubOpt{
		nats.AckWait(consumerCfg.GetAckWaitTiming()),
	}

	if consumerCfg.GetBackOffTimings() != nil {
		subOptions = append(subOptions,
			nats.BackOff(consumerCfg.GetBackOffTimings()),
			nats.MaxDeliver(consumerCfg.GetMaxDeliveryCount()),
		)
	}

	return &jsPushQueueGroupChanSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName:    consumerCfg.GetSubjectName(),
		queueGroupName: consumerCfg.GetQueueGroupName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),
		subscribeNatsOptions:   subOptions,

		msgChannel: msgChannel,
		logger:     l,
	}
}
