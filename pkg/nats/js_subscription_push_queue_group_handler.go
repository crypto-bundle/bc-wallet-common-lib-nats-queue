package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type jsPushQueueGroupHandlerSubscription struct {
	natsSubs  *nats.Subscription
	natsConn  *nats.Conn
	jsNatsCtx nats.JetStreamContext

	subjectName    string
	queueGroupName string

	autoReSubscribe        bool
	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration
	subscribeNatsOptions   []nats.SubOpt

	handler func(msg *nats.Msg)

	logger *zap.Logger
}

func (s *jsPushQueueGroupHandlerSubscription) OnReconnect(newConn *nats.Conn) error {
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

func (s *jsPushQueueGroupHandlerSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *jsPushQueueGroupHandlerSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) Shutdown(ctx context.Context) error {
	err := s.natsSubs.Unsubscribe()
	if err != nil {
		return err
	}

	s.handler = nil

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.QueueSubscribe(s.subjectName, s.queueGroupName,
		s.handler, s.subscribeNatsOptions...)
	if err != nil {
		return err
	}

	s.natsSubs = subs

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.jsNatsCtx.QueueSubscribe(s.subjectName, s.queueGroupName,
			s.handler, s.subscribeNatsOptions...)
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

func newJsPushQueueGroupHandlerSubscription(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,

	handler func(msg *nats.Msg),
) *jsPushQueueGroupHandlerSubscription {
	l := logger.Named("subscription")

	var subOptions []nats.SubOpt
	if consumerCfg.GetBackOffTimings() != nil {
		subOptions = append(subOptions,
			nats.BackOff(consumerCfg.GetBackOffTimings()),
			nats.MaxDeliver(consumerCfg.GetMaxDeliveryCount()),
		)
	}

	return &jsPushQueueGroupHandlerSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ Subscribe stage

		subjectName:    consumerCfg.GetSubjectName(),
		queueGroupName: consumerCfg.GetQueueGroupName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),
		subscribeNatsOptions:   subOptions,

		handler: handler,
		logger:  l,
	}
}
