package nats

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
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

	logger *log.Logger
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

func (s *jsPushQueueGroupHandlerSubscription) OnClosed(conn *nats.Conn) error {
	s.natsSubs = nil
	s.jsNatsCtx = nil
	s.natsConn = nil
	s.handler = nil

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *jsPushQueueGroupHandlerSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return err
	}

	s.jsNatsCtx = jsNatsCtx

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

func (s *jsPushQueueGroupHandlerSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return err
	}

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

func newJsPushQueueGroupHandlerSubscription(logger *log.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,

	handler func(msg *nats.Msg),
) *jsPushQueueGroupHandlerSubscription {
	subOptions := []nats.SubOpt{
		nats.AckWait(consumerCfg.GetAckWaitTiming()),
	}

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
		logger:  logger,
	}
}
