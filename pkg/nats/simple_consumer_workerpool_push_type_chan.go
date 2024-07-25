package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"log"
)

// simpleConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type simpleConsumerWorkerPool struct {
	handler consumerHandler
	workers []*consumerWorkerWrapper

	subscriptionSrv subscriptionService

	msgChannel chan *nats.Msg

	logger *log.Logger
}

func (wp *simpleConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Printf("consumer: unable to call onClosed in simple producer pool unit - %e", loopErr)

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
		wp.logger.Printf("consumer: unable to subscribe - %e", err)
	}

	go func() {
		<-ctx.Done()

		err = wp.subscriptionSrv.UnSubscribe()
		if err != nil {
			if err != nil {
				wp.logger.Printf("consumer: unable to unSubscribe - %e", err)
			}
		}

		wp.logger.Printf("consumer: successfully unSubscribed")
	}()

	return nil
}

func NewSimpleConsumerWorkersPool(logger *log.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *simpleConsumerWorkerPool {
	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	subscriptionSrv := newSimplePushQueueGroupSubscriptionService(logger, natsConn,
		consumerCfg, msgChannel)

	workersPool := &simpleConsumerWorkerPool{
		handler: handler,
		logger:  logger,

		subscriptionSrv: subscriptionSrv,

		msgChannel: msgChannel,
	}

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &consumerWorkerWrapper{
			msgChannel: msgChannel,
			handler:    workersPool.handler,
			logger:     logger,
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
