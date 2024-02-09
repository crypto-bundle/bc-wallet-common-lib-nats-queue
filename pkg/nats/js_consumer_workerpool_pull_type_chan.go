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

func (wp *jsPullTypeChannelConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Error("unable to call onClosed in consumer worker pool unit", zap.Error(loopErr))

			err = loopErr
		}
		wp.workers[i] = nil
	}

	wp.handler = nil
	close(wp.msgChannel)

	err = wp.pullSubscriber.OnClosed(conn)
	if err != nil {
		wp.logger.Error("unable to call onClosed in pull-type subscription service", zap.Error(err))
	}

	close(wp.msgChannel)
	wp.handler = nil
	wp.pullSubscriber = nil

	return err
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

	go func() {
		<-ctx.Done()

		err = wp.pullSubscriber.UnSubscribe()
		if err != nil {
			wp.logger.Error("unable to unSubscribe", zap.Error(err))
		}
	}()

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

	requeueDelays := consumerCfg.GetNakDelayTimings()

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel:        msgChannel,
			handler:           workersPool.handler,
			logger:            logger,
			reQueueDelay:      requeueDelays,
			reQueueDelayCount: uint64(len(requeueDelays) - 1),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
