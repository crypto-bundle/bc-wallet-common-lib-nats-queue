package nats

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

type jsPullChanSubscription struct {
	msgChannel chan *nats.Msg
	natsSubs   *nats.Subscription
	natsConn   *nats.Conn
	jsNatsCtx  nats.JetStreamContext

	subjectName string
	streamName  string
	durableName string
	durable     bool

	autoReSubscribe        bool
	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration

	fetchInterval time.Duration
	fetchTimeout  time.Duration
	fetchLimit    uint

	ticker *time.Ticker

	options []nats.SubOpt

	logger *zap.Logger
}

func (s *jsPullChanSubscription) OnReconnect(newConn *nats.Conn) error {
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

func (s *jsPullChanSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPullChanSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *jsPullChanSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPullChanSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.PullSubscribe(s.subjectName, s.durableName, s.options...)
	if err != nil {
		return err
	}

	s.natsSubs = subs
	s.ticker = time.NewTicker(s.fetchInterval)

	go s.run(ctx)

	return nil
}

func (s *jsPullChanSubscription) run(ctx context.Context) {
	for {
		select {
		case <-s.ticker.C:
			msgList, fetchErr := s.natsSubs.Fetch(int(s.fetchLimit),
				nats.MaxWait(s.fetchTimeout))

			if fetchErr == nil {
				for i := 0; i != len(msgList); i++ {
					s.msgChannel <- msgList[i]
				}

				continue
			}

			if fetchErr != nil && errors.Is(fetchErr, nats.ErrTimeout) {
				continue
			}

			s.logger.Error("unable fetch data", zap.Error(fetchErr))
		}
	}
}

func (s *jsPullChanSubscription) Shutdown(ctx context.Context) error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

	s.natsSubs = nil
	s.ticker.Stop()
	s.ticker = nil

	return nil
}

func (s *jsPullChanSubscription) onDisconnect(conn *nats.Conn, err error) {
	s.ticker.Stop()

	return
}

func (s *jsPullChanSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.jsNatsCtx.PullSubscribe(s.subjectName, s.durableName, s.options...)
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

func newJsPullChanSubscriptionService(logger *zap.Logger,
	natsConn *nats.Conn,

	subjectName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	fetchInterval time.Duration,
	fetchTimeout time.Duration,
	fetchLimit uint,
	msgChannel chan *nats.Msg,
) *jsPullChanSubscription {
	l := logger.Named("subscription")

	return &jsPullChanSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName: subjectName,

		autoReSubscribe:        autoReSubscribe,
		autoReSubscribeCount:   autoReSubscribeCount,
		autoReSubscribeTimeout: autoReSubscribeTimeout,

		fetchInterval: fetchInterval,
		fetchLimit:    fetchLimit,
		fetchTimeout:  fetchTimeout,

		msgChannel: msgChannel,

		logger: l,
	}
}