package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsPushTypeQueueGroupChannelConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type jsPushTypeQueueGroupChannelConsumerWorkerPool struct {
	handler consumerHandler
	workers []*jsConsumerWorkerWrapper

	subscriptionSvc subscriptionService

	msgChannel chan *nats.Msg

	logger *zap.Logger
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Error("unable to call onClosed in consumer worker pool unit", zap.Error(loopErr))

			err = loopErr
		}
		wp.workers[i] = nil
	}

	err = wp.subscriptionSvc.OnClosed(conn)
	if err != nil {
		wp.logger.Error("unable to call onClosed in subscription service", zap.Error(err))
	}

	close(wp.msgChannel)
	wp.handler = nil
	wp.subscriptionSvc = nil

	return err
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) OnReconnect(conn *nats.Conn) error {
	err := wp.subscriptionSvc.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSvc.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSvc.Healthcheck(ctx)
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) Init(ctx context.Context) error {
	return wp.subscriptionSvc.Init(ctx)
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) Run(ctx context.Context) error {
	for _, w := range wp.workers {
		go w.Run(ctx)
	}

	err := wp.subscriptionSvc.Subscribe(ctx)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()

		err = wp.subscriptionSvc.UnSubscribe()
		if err != nil {
			wp.logger.Error("unable to unSubscribe", zap.Error(err))
		}

		wp.logger.Info("successfully unsubscribed")

		return
	}()

	return nil
}

func NewJsPushTypeChannelGroupConsumerWorkersPool(logger *zap.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsPushTypeQueueGroupChannelConsumerWorkerPool {
	l := logger.Named("queue_consumer_pool.service")

	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	subscriptionSrv := newJsPushQueueGroupChanSubscriptionService(l, natsConn, consumerCfg,
		msgChannel)

	workersPool := &jsPushTypeQueueGroupChannelConsumerWorkerPool{
		handler:         handler,
		logger:          l,
		subscriptionSvc: subscriptionSrv,
		msgChannel:      msgChannel,
	}

	requeueDelays := consumerCfg.GetNakDelayTimings()

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel:        msgChannel,
			handler:           workersPool.handler,
			logger:            l.With(zap.Uint32(WorkerUnitNumberTag, i)),
			reQueueDelay:      requeueDelays,
			reQueueDelayCount: uint64(len(requeueDelays) - 1),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
