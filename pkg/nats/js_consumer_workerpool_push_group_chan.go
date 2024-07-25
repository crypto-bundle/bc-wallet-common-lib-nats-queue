package nats

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

// jsPushTypeQueueGroupChannelConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type jsPushTypeQueueGroupChannelConsumerWorkerPool struct {
	handler consumerHandler
	workers []*jsConsumerWorkerWrapper

	subscriptionSvc subscriptionService

	msgChannel chan *nats.Msg

	logger *log.Logger
}

func (wp *jsPushTypeQueueGroupChannelConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Printf("consumer: unable to call onClosed in consumer worker pool unit - %e", loopErr)

			err = loopErr
		}
		wp.workers[i] = nil
	}

	err = wp.subscriptionSvc.OnClosed(conn)
	if err != nil {
		wp.logger.Printf("consumer: unable to call onClosed in subscription service - %e", err)
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
			wp.logger.Printf("consumer: unable to unSubscribe - %e", err)
		}

		wp.logger.Printf("consumer: successfully unSubscribed")

		return
	}()

	return nil
}

func NewJsPushTypeChannelGroupConsumerWorkersPool(logger *log.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsPushTypeQueueGroupChannelConsumerWorkerPool {
	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	subscriptionSrv := newJsPushQueueGroupChanSubscriptionService(logger, natsConn, consumerCfg,
		msgChannel)

	workersPool := &jsPushTypeQueueGroupChannelConsumerWorkerPool{
		handler:         handler,
		logger:          logger,
		subscriptionSvc: subscriptionSrv,
		msgChannel:      msgChannel,
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
