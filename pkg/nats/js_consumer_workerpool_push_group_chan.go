package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsPushTypeChannelConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type jsPushTypeChannelConsumerWorkerPool struct {
	handler consumerHandler
	workers []*jsConsumerWorkerWrapper

	subscriptionSrv subscriptionService

	msgChannel chan *nats.Msg

	logger *zap.Logger
}

func (wp *jsPushTypeChannelConsumerWorkerPool) OnReconnect(conn *nats.Conn) error {
	err := wp.subscriptionSrv.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPushTypeChannelConsumerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSrv.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsPushTypeChannelConsumerWorkerPool) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSrv.Healthcheck(ctx)
}

func (wp *jsPushTypeChannelConsumerWorkerPool) Init(ctx context.Context) error {
	return wp.subscriptionSrv.Init(ctx)
}

func (wp *jsPushTypeChannelConsumerWorkerPool) Run(ctx context.Context) error {
	for _, w := range wp.workers {
		go w.Run(ctx)
	}

	return wp.subscriptionSrv.Subscribe(ctx)
}

func (wp *jsPushTypeChannelConsumerWorkerPool) Shutdown(ctx context.Context) error {
	for _, w := range wp.workers {
		w.Stop()
	}

	err := wp.subscriptionSrv.Shutdown(ctx)
	if err != nil {
		wp.logger.Warn("unable to shutdown subscription")
	}

	wp.handler = nil

	close(wp.msgChannel)
	wp.msgChannel = nil

	return nil
}

func NewJsPushTypeChannelConsumerWorkersPool(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsPushTypeChannelConsumerWorkerPool {
	l := logger.Named("queue_consumer_pool.service")

	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	subscriptionSrv := newJsPushQueueGroupChanSubscriptionService(l, natsConn, consumerCfg,
		msgChannel)

	workersPool := &jsPushTypeChannelConsumerWorkerPool{
		handler:         handler,
		logger:          l,
		subscriptionSrv: subscriptionSrv,
	}

	for i := uint(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel:       msgChannel,
			stopWorkerChanel: make(chan bool),
			handler:          workersPool.handler,
			logger:           l.With(zap.Uint(WorkerUnitNumberTag, i)),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
