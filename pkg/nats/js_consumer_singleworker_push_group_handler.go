package nats

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"time"

	"go.uber.org/zap"
)

var (
	ErrReturnedNilConsumerInfo = errors.New("returned nil consumer info")
	ErrReturnedNilStreamInfo   = errors.New("returned nil stream info")
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

	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
	subOpt ...nats.SubOpt,
) *jsConsumerPushQueueGroupSingeWorker {
	l := logger.Named("consumer_worker_pool")

	ww := &jsConsumerWorkerWrapper{
		msgChannel:       nil, // cuz channel-less single-worker worker pool
		stopWorkerChanel: nil, // cuz channel-less single-worker worker pool
		logger:           logger,
		handler:          handler,
	}

	subscriptionSrv := newJsPushQueueGroupHandlerSubscription(logger, natsConn,
		subjectName, queueGroupName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout, ww.ProcessMsg,
		subOpt...,
	)

	workersPool := &jsConsumerPushQueueGroupSingeWorker{
		logger:          l,
		subscriptionSrv: subscriptionSrv,
		worker:          ww,
	}

	return workersPool
}
