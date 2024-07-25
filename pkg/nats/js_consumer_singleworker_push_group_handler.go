package nats

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

// jsPushTypeQueueGroupConsumer is a minimal Worker implementation that simply wraps a
type jsConsumerPushQueueGroupSingeWorker struct {
	subscriptionSvc subscriptionService

	worker *jsConsumerWorkerWrapper

	logger *log.Logger
}

func (wp *jsConsumerPushQueueGroupSingeWorker) OnReconnect(conn *nats.Conn) error {
	err := wp.subscriptionSvc.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSvc.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) OnClosed(conn *nats.Conn) error {
	err := wp.subscriptionSvc.OnClosed(conn)
	if err != nil {
		wp.logger.Printf("consumer: unable to call onClosed in pull-type subscription service - %e", err)
	}

	wp.subscriptionSvc = nil

	return err
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Init(ctx context.Context) error {
	err := wp.subscriptionSvc.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSvc.Healthcheck(ctx)
}

func (wp *jsConsumerPushQueueGroupSingeWorker) Run(ctx context.Context) error {
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
	}()

	return nil
}

func NewJsConsumerPushQueueGroupSingeWorker(logger *log.Logger,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsConsumerPushQueueGroupSingeWorker {
	requeueDelays := consumerCfg.GetNakDelayTimings()

	ww := &jsConsumerWorkerWrapper{
		msgChannel:        nil, // cuz channel-less single-worker worker pool
		logger:            logger,
		handler:           handler,
		reQueueDelay:      requeueDelays,
		reQueueDelayCount: uint64(len(requeueDelays) - 1),
	}

	subscriptionSrv := newJsPushQueueGroupHandlerSubscription(logger, natsConn, consumerCfg, ww.ProcessMsg)

	workersPool := &jsConsumerPushQueueGroupSingeWorker{
		logger:          logger,
		subscriptionSvc: subscriptionSrv,
		worker:          ww,
	}

	return workersPool
}
