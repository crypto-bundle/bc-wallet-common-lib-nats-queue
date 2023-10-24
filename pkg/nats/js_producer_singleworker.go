package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsProducerSingleWorker ...
type jsProducerSingleWorker struct {
	logger *zap.Logger

	streamName string
	subjects   []string

	natsProducerConn *nats.Conn
	jsCtx            nats.JetStreamContext
}

func (sw *jsProducerSingleWorker) OnReconnect(newConn *nats.Conn) error {
	sw.natsProducerConn = newConn

	jsNatsCtx, err := newConn.JetStream()
	if err != nil {
		return err
	}

	sw.jsCtx = jsNatsCtx

	return nil
}

func (sw *jsProducerSingleWorker) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (sw *jsProducerSingleWorker) Healthcheck(ctx context.Context) bool {
	if !sw.natsProducerConn.IsConnected() {
		sw.logger.Warn("producer lost nats originConn")

		return false
	}

	return true
}

func (sw *jsProducerSingleWorker) Init(ctx context.Context) error {
	jsNatsCtx, err := sw.natsProducerConn.JetStream()
	if err != nil {
		return err
	}

	sw.jsCtx = jsNatsCtx

	return nil
}

func (sw *jsProducerSingleWorker) Run(ctx context.Context) error {
	return nil
}

func (sw *jsProducerSingleWorker) Shutdown(ctx context.Context) error {
	return nil
}

func (sw *jsProducerSingleWorker) Produce(ctx context.Context, msg *nats.Msg) {
	err := sw.produce(ctx, msg)
	if err != nil {
		sw.logger.Error("unable to produce nats message", zap.Error(err))
	}
}

func (sw *jsProducerSingleWorker) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	err := sw.produce(ctx, msg)
	if err != nil {
		sw.logger.Error("unable to produce nats message", zap.Error(err))

		return err
	}

	return nil
}

func (sw *jsProducerSingleWorker) produce(ctx context.Context, msg *nats.Msg) error {
	pubAck, err := sw.jsCtx.PublishMsg(msg)
	if err != nil {
		return err
	}

	if pubAck == nil {
		sw.logger.Error("received nil pubAck", zap.Error(ErrNilPubAck))

		return ErrNilPubAck
	}

	return nil
}

func NewJsProducerSingleWorkerService(logger *zap.Logger,
	natsProducerConn *nats.Conn,
	streamName string,
	subjects []string,
) *jsProducerSingleWorker {
	l := logger.Named("producer.service").
		With(zap.String(QueueStreamNameTag, streamName))

	workersPool := &jsProducerSingleWorker{
		logger:     l,
		streamName: streamName,
		subjects:   subjects,

		natsProducerConn: natsProducerConn,
		jsCtx:            nil, // will be filed @ init stage
	}

	return workersPool
}
