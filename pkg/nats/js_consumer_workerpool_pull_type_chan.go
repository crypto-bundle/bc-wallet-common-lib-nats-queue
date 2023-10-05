package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsPullTypeChannelConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type jsPullTypeChannelConsumerWorkerPool struct {
	msgChannel chan *nats.Msg

	subjectName string

	pullSubscriber subscriptionService

	handler consumerHandler
	workers []*jsConsumerWorkerWrapper

	logger *zap.Logger
}

func (wp *jsPullTypeChannelConsumerWorkerPool) OnReconnect(conn *nats.Conn) error {
	err := wp.pullSubscriber.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.pullSubscriber.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Healthcheck(ctx context.Context) bool {
	return wp.pullSubscriber.Healthcheck(ctx)
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Init(ctx context.Context) error {
	err := wp.pullSubscriber.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Run(ctx context.Context) error {
	for _, w := range wp.workers {
		w.msgChannel = wp.msgChannel

		go w.Run(ctx)
	}

	err := wp.pullSubscriber.Subscribe(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Shutdown(ctx context.Context) error {
	for _, w := range wp.workers {
		w.Stop()
	}

	err := wp.pullSubscriber.Shutdown(ctx)
	if err != nil {
		wp.logger.Warn("unable to shutdown unsubscribe")
		return err
	}

	wp.handler = nil

	close(wp.msgChannel)
	wp.msgChannel = nil

	return nil
}

func NewJsPullTypeConsumerWorkersPool(logger *zap.Logger,
	jsNatsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeChannelConsumerWorkerPool {
	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	pullSubscriber := newJsPullChanSubscriptionService(logger, jsNatsConn, consumerCfg, msgChannel)

	workersPool := &jsPullTypeChannelConsumerWorkerPool{
		handler:        handler,
		logger:         logger,
		msgChannel:     msgChannel,
		subjectName:    consumerCfg.GetSubjectName(),
		pullSubscriber: pullSubscriber,
	}

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel:       msgChannel,
			stopWorkerChanel: make(chan bool),
			handler:          workersPool.handler,
			logger:           logger,
			reQueueDelay:     consumerCfg.GetNakDelay(),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
