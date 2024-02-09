package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsPullTypeHandlerConsumer is a minimal Worker implementation that simply wraps a
type jsPullTypeHandlerConsumer struct {
	pullSubscriber subscriptionService

	worker *jsConsumerWorkerWrapper

	logger *zap.Logger
}

func (wp *jsPullTypeHandlerConsumer) OnClosed(conn *nats.Conn) error {
	var err error

	err = wp.pullSubscriber.OnClosed(conn)
	if err != nil {
		wp.logger.Error("unable to call onClosed in pull-type subscription service", zap.Error(err))
	}

	wp.pullSubscriber = nil

	return err
}

func (wp *jsPullTypeHandlerConsumer) OnReconnect(conn *nats.Conn) error {
	err := wp.pullSubscriber.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeHandlerConsumer) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.pullSubscriber.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsPullTypeHandlerConsumer) Healthcheck(ctx context.Context) bool {
	return wp.pullSubscriber.Healthcheck(ctx)
}

func (wp *jsPullTypeHandlerConsumer) Init(ctx context.Context) error {
	err := wp.pullSubscriber.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeHandlerConsumer) Run(ctx context.Context) error {
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

func NewJsPullTypeHandlerConsumer(logger *zap.Logger,
	jsNatsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeHandlerConsumer {

	requeueDelays := consumerCfg.GetNakDelayTimings()

	ww := &jsConsumerWorkerWrapper{
		msgChannel:        nil, // cuz channel-less single-worker worker pool
		logger:            logger,
		handler:           handler,
		reQueueDelay:      requeueDelays,
		reQueueDelayCount: uint64(len(requeueDelays) - 1),
	}

	pullSubscriber := newJsPullHandlerSubscriptionService(logger, jsNatsConn,
		consumerCfg, ww.ProcessMsg)

	return &jsPullTypeHandlerConsumer{
		pullSubscriber: pullSubscriber,
		worker:         ww,
		logger:         logger,
	}
}
