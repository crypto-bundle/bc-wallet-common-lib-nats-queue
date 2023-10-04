package nats

import (
	"context"
	"time"
	
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type jsPushSubscription struct {
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

func (s *jsPushSubscription) OnReconnect(newConn *nats.Conn) error {
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

func (s *jsPushSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPushSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *jsPushSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPushSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.ChanSubscribe(s.subjectName, s.msgChannel)
	if err != nil {
		return err
	}

	s.natsSubs = subs

	//s.natsConn.SetErrorHandler(s.onDisconnect)
	//s.natsConn.SetReconnectHandler(s.tryResubscribe)

	return nil
}

func (s *jsPushSubscription) Shutdown(ctx context.Context) error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

	return nil
}

func (s *jsPushSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.jsNatsCtx.ChanSubscribe(s.subjectName, s.msgChannel, s.subscribeNatsOptions...)
		if subsErr != nil {
			s.logger.Warn("unable to re-subscribe", zap.Error(subsErr),
				zap.Uint16(ResubscribeTag, i))

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

func newJsPushSubscriptionService(logger *zap.Logger,
	natsConn *nats.Conn,

	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	msgChannel chan *nats.Msg,
	subOpt ...nats.SubOpt,
) *jsPushSubscription {
	l := logger.Named("subscription")

	return &jsPushSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName:    subjectName,
		queueGroupName: queueGroupName,

		autoReSubscribe:        autoReSubscribe,
		autoReSubscribeCount:   autoReSubscribeCount,
		autoReSubscribeTimeout: autoReSubscribeTimeout,
		subscribeNatsOptions:   subOpt,

		msgChannel: msgChannel,
		logger:     l,
	}
}
