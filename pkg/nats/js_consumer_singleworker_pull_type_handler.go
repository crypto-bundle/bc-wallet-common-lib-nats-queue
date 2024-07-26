package nats

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

// jsPullTypeHandlerConsumer is a minimal Worker implementation that simply wraps a
type jsPullTypeHandlerConsumer struct {
	pullSubscriber subscriptionService

	worker *jsConsumerWorkerWrapper

	logger *log.Logger
}

func (wp *jsPullTypeHandlerConsumer) OnClosed(conn *nats.Conn) error {
	var err error

	err = wp.pullSubscriber.OnClosed(conn)
	if err != nil {
		wp.logger.Printf("consumer: unable to call onClosed in pull-type subscription service - %e", err)
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
			wp.logger.Printf("consumer: unable to unSubscribe - %e", err)
		}
	}()

	return nil
}

func NewJsPullTypeHandlerConsumer(loggerFactorySvc loggerService,
	jsNatsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeHandlerConsumer {

	requeueDelays := consumerCfg.GetNakDelayTimings()

	ww := &jsConsumerWorkerWrapper{
		msgChannel: nil, // cuz channel-less single-worker worker pool
		logger: loggerFactorySvc.WithFields("nats", map[string]interface{}{
			natsFunctionalUnitTag: natsJetStreamConsumerUnitNameTag,
		}),
		handler:           handler,
		reQueueDelay:      requeueDelays,
		reQueueDelayCount: uint64(len(requeueDelays) - 1),
	}

	pullSubscriber := newJsPullHandlerSubscriptionService(loggerFactorySvc.WithFields("nats",
		map[string]interface{}{
			natsFunctionalUnitTag: natsJetStreamSubscriptionUnitNameTag,
			natsConsumerTypeTag:   natsPullTypeQueueGroupConsumerNameTag,
		}), jsNatsConn,
		consumerCfg, ww.ProcessMsg)

	return &jsPullTypeHandlerConsumer{
		pullSubscriber: pullSubscriber,
		worker:         ww,
		logger: loggerFactorySvc.WithFields("nats", map[string]interface{}{
			natsFunctionalUnitTag: natsSubscriptionUnitWorkerNameTag,
			natsConsumerTypeTag:   natsPullTypeQueueGroupConsumerNameTag,
		}),
	}
}
