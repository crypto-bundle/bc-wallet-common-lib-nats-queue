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

	msgChannel chan *nats.Msg

	logger *zap.Logger
}

func (s *jsPushQueueGroupChanSubscription) Healthcheck(ctx context.Context) bool {
	if !s.natsConn.IsConnected() {
		s.logger.Warn("consumer lost nats connection")

		return false
	}

	if !s.natsSubs.IsValid() {
		s.logger.Warn("consumer lost nats subscription")

		s.natsSubs.IsValid()

		if s.autoReSubscribe {
			return s.tryResubscribe(ctx)
		}

		return false
	}

	return true
}

func (s *jsPushQueueGroupChanSubscription) Init(ctx context.Context) error {
	return nil
}

func (s *jsPushQueueGroupChanSubscription) Shutdown(ctx context.Context) error {
	return s.natsSubs.Unsubscribe()
}

func (s *jsPushQueueGroupChanSubscription) Run(ctx context.Context) error {
	subs, err := s.jsNatsCtx.ChanQueueSubscribe(s.subjectName, s.queueGroupName, s.msgChannel)
	if err != nil {
		return err
	}

	s.natsSubs = subs

	return nil
}

func (s *jsPushQueueGroupChanSubscription) tryResubscribe(ctx context.Context) bool {
	var isSubscribed = false

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, err := s.jsNatsCtx.ChanQueueSubscribe(s.subjectName, s.queueGroupName, s.msgChannel)
		if err != nil {
			s.logger.Warn("unable to re-subscribe", zap.Error(err),
				zap.Uint16(ResubscribeTag, i))

			time.Sleep(s.autoReSubscribeTimeout)
			continue
		}

		s.natsSubs = subs
		isSubscribed = true

		s.logger.Info("re-subscription success")
		break
	}

	return isSubscribed
}

func newJsPushQueueGroupChanSubscriptionService(logger *zap.Logger,
	natsConn *nats.Conn,

	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	msgChannel chan *nats.Msg,
) *jsPushQueueGroupChanSubscription {
	l := logger.Named("subscription")

	return &jsPushQueueGroupChanSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName:    subjectName,
		queueGroupName: queueGroupName,

		autoReSubscribe:        autoReSubscribe,
		autoReSubscribeCount:   autoReSubscribeCount,
		autoReSubscribeTimeout: autoReSubscribeTimeout,

		msgChannel: msgChannel,
		logger:     l,
	}
}
