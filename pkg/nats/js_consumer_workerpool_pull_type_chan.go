package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var (
	ErrNilSubscribeInfo = errors.New("receive nil subscribe info")
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

	workersCount uint16,
	subjectName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	fetchInterval time.Duration,
	fetchTimeout time.Duration,
	fetchLimit uint,

	handler consumerHandler,
) *jsPullTypeChannelConsumerWorkerPool {
	msgChannel := make(chan *nats.Msg, workersCount)

	pullSubscriber := newJsPullChanSubscriptionService(logger, jsNatsConn,
		subjectName, autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		fetchInterval, fetchTimeout, fetchLimit, msgChannel)

	workersPool := &jsPullTypeChannelConsumerWorkerPool{
		handler:        handler,
		logger:         logger,
		msgChannel:     msgChannel,
		subjectName:    subjectName,
		pullSubscriber: pullSubscriber,
	}

	for i := uint16(0); i < workersCount; i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel:       msgChannel,
			stopWorkerChanel: make(chan bool),
			handler:          workersPool.handler,
			logger:           logger,
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}