package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// simpleConsumerSingeWorker is a minimal Worker implementation that simply wraps a
type simpleConsumerSingeWorker struct {
	subscriptionSrv subscriptionService

	handler func(msg *nats.Msg)
	worker  *jsConsumerWorkerWrapper

	logger *zap.Logger
}

func (wp *simpleConsumerSingeWorker) OnReconnect(conn *nats.Conn) error {
	err := wp.subscriptionSrv.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSrv.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) Init(ctx context.Context) error {
	err := wp.subscriptionSrv.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSrv.Healthcheck(ctx)
}

func (wp *simpleConsumerSingeWorker) Run(ctx context.Context) error {
	err := wp.subscriptionSrv.Subscribe(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) Shutdown(ctx context.Context) error {
	err := wp.subscriptionSrv.Shutdown(ctx)
	if err != nil {
		return err
	}

	wp.handler = nil

	return nil
}

func NewSimpleConsumerSingeWorker(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsConsumerPushQueueGroupSingeWorker {
	l := logger.Named("consumer_worker_pool")

	ww := &jsConsumerWorkerWrapper{
		msgChannel:       nil, // cuz channel-less single-worker worker pool
		stopWorkerChanel: nil, // cuz channel-less single-worker worker pool
		logger:           logger,
		handler:          handler,
		reQueueDelay:     consumerCfg.GetNakDelayTimings(),
	}

	subscriptionSrv := newSimplePushSubscriptionService(logger, natsConn, consumerCfg, ww.ProcessMsg)

	workersPool := &jsConsumerPushQueueGroupSingeWorker{
		logger:          l,
		subscriptionSrv: subscriptionSrv,
		worker:          ww,
	}

	return workersPool
}
