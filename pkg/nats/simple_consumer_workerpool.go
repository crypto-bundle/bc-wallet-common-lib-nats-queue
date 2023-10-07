package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// simpleConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type simpleConsumerWorkerPool struct {
	handler consumerHandler
	workers []*consumerWorkerWrapper

	subscriptionSrv subscriptionService

	msgChannel chan *nats.Msg

	logger *zap.Logger
}

func (wp *simpleConsumerWorkerPool) OnReconnect(conn *nats.Conn) error {
	retErr := wp.subscriptionSrv.OnReconnect(conn)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *simpleConsumerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSrv.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *simpleConsumerWorkerPool) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSrv.Healthcheck(ctx)
}

func (wp *simpleConsumerWorkerPool) Init(ctx context.Context) error {
	return wp.subscriptionSrv.Init(ctx)
}

func (wp *simpleConsumerWorkerPool) Run(ctx context.Context) error {
	wp.run()

	return wp.subscriptionSrv.Subscribe(ctx)
}

func (wp *simpleConsumerWorkerPool) run() {
	for _, w := range wp.workers {
		go w.Run()
	}
}

func (wp *simpleConsumerWorkerPool) Shutdown(ctx context.Context) error {
	for _, w := range wp.workers {
		w.Stop()
	}

	close(wp.msgChannel)
	wp.msgChannel = nil

	return nil
}

func NewSimpleConsumerWorkersPool(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *simpleConsumerWorkerPool {
	l := logger.Named("consumer_pool")

	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	subscriptionSrv := newSimplePushQueueGroupSubscriptionService(l, natsConn,
		consumerCfg, msgChannel)

	workersPool := &simpleConsumerWorkerPool{
		handler: handler,
		logger:  l,

		subscriptionSrv: subscriptionSrv,

		msgChannel: msgChannel,
	}

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &consumerWorkerWrapper{
			msgChannel:       msgChannel,
			stopWorkerChanel: make(chan bool),
			handler:          workersPool.handler,
			logger:           l.With(zap.Uint32(WorkerUnitNumberTag, i)),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
