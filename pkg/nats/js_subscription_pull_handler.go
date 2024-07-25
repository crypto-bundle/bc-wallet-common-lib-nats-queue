package nats

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

type jsPullHandlerSubscription struct {
	natsSubs    *nats.Subscription
	natsConn    *nats.Conn
	jsNatsCtx   nats.JetStreamContext
	subjectName string

	streamName      string
	durableName     string
	autoReSubscribe bool

	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration
	subscribeNatsOptions   []nats.SubOpt
	fetchInterval          time.Duration

	fetchTimeout time.Duration
	fetchLimit   uint

	ticker *time.Ticker

	logger *log.Logger

	handler func(msg *nats.Msg)
}

func (s *jsPullHandlerSubscription) OnClosed(conn *nats.Conn) error {
	s.natsConn = nil
	s.natsSubs = nil
	s.jsNatsCtx = nil
	s.ticker = nil

	return nil
}

func (s *jsPullHandlerSubscription) OnReconnect(newConn *nats.Conn) error {
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

func (s *jsPullHandlerSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPullHandlerSubscription) Healthcheck(ctx context.Context) bool {
	if !s.natsConn.IsConnected() {
		s.logger.Print("subscription: lost NATS origin connection")

		return false
	}

	if !s.natsSubs.IsValid() {
		s.logger.Print("subscription: lost NATS subscription")

		return false
	}

	return true
}

func (s *jsPullHandlerSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPullHandlerSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.PullSubscribe(s.subjectName, s.durableName, s.subscribeNatsOptions...)
	if err != nil {
		return err
	}

	s.natsSubs = subs
	s.ticker = time.NewTicker(s.fetchInterval)

	go s.run(ctx)

	return nil
}

func (s *jsPullHandlerSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

	s.ticker.Stop()

	return nil
}

func (s *jsPullHandlerSubscription) run(ctx context.Context) {
	for {
		select {
		case <-s.ticker.C:
			msgList, fetchErr := s.natsSubs.Fetch(int(s.fetchLimit),
				nats.MaxWait(s.fetchTimeout))

			if fetchErr == nil {
				for i := 0; i != len(msgList); i++ {
					s.handler(msgList[i])
				}

				continue
			}

			if fetchErr != nil && errors.Is(fetchErr, nats.ErrTimeout) {
				continue
			}

			s.logger.Printf("subscription: unable fetch data - %e", fetchErr)

		case <-ctx.Done():
			s.logger.Print("subscription: received close message")

			return
		}
	}
}

func (s *jsPullHandlerSubscription) onDisconnect(conn *nats.Conn, err error) {
	s.ticker.Stop()

	return
}

func (s *jsPullHandlerSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.jsNatsCtx.PullSubscribe(s.subjectName, s.durableName, s.subscribeNatsOptions...)
		if subsErr != nil {
			s.logger.Printf("subscription: unable to re-subscribe - %s: %d, error: %e",
				ResubscribeTag, i, subsErr)

			err = subsErr

			time.Sleep(s.autoReSubscribeTimeout)
			continue
		}

		s.natsSubs = subs

		s.logger.Print("subscription: re-subscription success")

		return nil
	}

	if err != nil {
		return err
	}

	return nil
}

func newJsPullHandlerSubscriptionService(logger *log.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler func(msg *nats.Msg),
) *jsPullHandlerSubscription {
	subOptions := []nats.SubOpt{
		nats.AckWait(consumerCfg.GetAckWaitTiming()),
	}

	if consumerCfg.GetBackOffTimings() != nil {
		subOptions = append(subOptions,
			nats.BackOff(consumerCfg.GetBackOffTimings()),
			nats.MaxDeliver(consumerCfg.GetMaxDeliveryCount()),
		)
	}

	return &jsPullHandlerSubscription{
		natsConn:  natsConn,
		jsNatsCtx: nil,
		natsSubs:  nil, // it will be set @ run stage

		subjectName: consumerCfg.GetSubjectName(),
		durableName: consumerCfg.GetDurableName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),
		subscribeNatsOptions:   subOptions,

		fetchInterval: consumerCfg.GetFetchInterval(),
		fetchTimeout:  consumerCfg.GetFetchTimeout(),
		fetchLimit:    consumerCfg.GetFetchLimit(),

		handler: handler,

		logger: logger,
	}
}
