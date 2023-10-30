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

func (wp *simpleConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Error("unable to call onClosed in simple producer pool unit", zap.Error(loopErr))

			err = loopErr
		}

		wp.workers[i] = nil
	}

	close(wp.msgChannel)
	wp.msgChannel = nil

	return err
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
	for _, w := range wp.workers {
		go w.Run(ctx)
	}

	err := wp.subscriptionSrv.Subscribe(ctx)
	if err != nil {
		wp.logger.Error("unable to subscribe", zap.Error(err))
	}

	go func() {
		<-ctx.Done()

		err = wp.subscriptionSrv.UnSubscribe()
		if err != nil {
			wp.logger.Error("unable to unSubscribe", zap.Error(err))
		}
	}()

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
			msgChannel: msgChannel,
			handler:    workersPool.handler,
			logger:     l.With(zap.Uint32(WorkerUnitNumberTag, i)),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
