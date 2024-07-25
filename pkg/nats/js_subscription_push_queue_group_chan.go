package nats

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
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

	logger *log.Logger
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
		s.logger.Print("subscription: lost NATS origin connection")

		return false
	}

	if !s.natsSubs.IsValid() {
		s.logger.Print("subscription: lost NATS subscription")

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

func newJsPushQueueGroupChanSubscriptionService(logger *log.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	msgChannel chan *nats.Msg,
) *jsPushQueueGroupChanSubscription {

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
		logger:     logger,
	}
}
