package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"time"
)

// jsQueueConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type jsQueueConsumerWorkerPool struct {
	handler consumerHandler
	workers []*jsConsumerWorkerWrapper

	subscriptionSrv subscriptionService

	logger *zap.Logger
}

func (wp *jsQueueConsumerWorkerPool) Init(ctx context.Context) error {
	return wp.subscriptionSrv.Init(ctx)
}

func (wp *jsQueueConsumerWorkerPool) Run(ctx context.Context) error {
	wp.run(ctx)

	return wp.subscriptionSrv.Run(ctx)
}

func (wp *jsQueueConsumerWorkerPool) run(ctx context.Context) {
	for _, w := range wp.workers {
		go w.Run(ctx)
	}
}

func (wp *jsQueueConsumerWorkerPool) Shutdown(ctx context.Context) error {
	for _, w := range wp.workers {
		w.Stop()
	}

	return nil
}

func NewJsQueueConsumerWorkersPool(logger *zap.Logger,
	msgChannel chan *nats.Msg,
	streamName string,

	workersCount uint16,
	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
	natsConn *nats.Conn,
	jsNatsConn nats.JetStreamContext,
) *jsQueueConsumerWorkerPool {
	l := logger.Named("queue_consumer_pool.service")

	subscriptionSrv := newJsPushSubscriptionService(l, natsConn, subjectName,
		queueGroupName, autoReSubscribe,
		autoReSubscribeCount, autoReSubscribeTimeout, msgChannel)

	workersPool := &jsQueueConsumerWorkerPool{
		handler:         handler,
		logger:          l,
		subscriptionSrv: subscriptionSrv,
	}

	for i := uint16(0); i < workersCount; i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel:       msgChannel,
			stopWorkerChanel: make(chan bool),
			handler:          workersPool.handler,
			logger:           l.With(zap.Uint16(WorkerUnitNumberTag, i)),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
