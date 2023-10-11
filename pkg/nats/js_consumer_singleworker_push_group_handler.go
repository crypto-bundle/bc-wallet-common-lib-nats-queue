package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsPushTypeQueueGroupConsumer is a minimal Worker implementation that simply wraps a
type jsConsumerPushQueueGroupSingeWorker struct {
	subscriptionSrv subscriptionService

	handler func(msg *nats.Msg)
	worker  *jsConsumerWorkerWrapper

	logger *zap.Logger
}

func (wp *jsConsumerPushQueueGroupSingeWorker) OnReconnect(conn *nats.Conn) error {
	err := wp.subscriptionSrv.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSrv.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Init(ctx context.Context) error {
	err := wp.subscriptionSrv.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSrv.Healthcheck(ctx)
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Run(ctx context.Context) error {
	err := wp.subscriptionSrv.Subscribe(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Shutdown(ctx context.Context) error {
	err := wp.subscriptionSrv.Shutdown(ctx)
	if err != nil {
		return err
	}

	wp.handler = nil

	return nil
}

func NewJsConsumerPushQueueGroupSingeWorker(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsConsumerPushQueueGroupSingeWorker {
	l := logger.Named("consumer_worker_pool")

	requeueDelays := consumerCfg.GetNakDelayTimings()

	ww := &jsConsumerWorkerWrapper{
		msgChannel:        nil, // cuz channel-less single-worker worker pool
		stopWorkerChanel:  nil, // cuz channel-less single-worker worker pool
		logger:            logger,
		handler:           handler,
		reQueueDelay:      requeueDelays,
		reQueueDelayCount: uint64(len(requeueDelays)),
	}

	subscriptionSrv := newJsPushQueueGroupHandlerSubscription(logger, natsConn, consumerCfg, ww.ProcessMsg)

	workersPool := &jsConsumerPushQueueGroupSingeWorker{
		logger:          l,
		subscriptionSrv: subscriptionSrv,
		worker:          ww,
	}

	return workersPool
}
