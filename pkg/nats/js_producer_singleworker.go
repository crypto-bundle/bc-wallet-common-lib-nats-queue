package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"log"
)

// jsProducerSingleWorker ...
type jsProducerSingleWorker struct {
	logger *log.Logger

	streamName string
	subjects   []string

	natsProducerConn *nats.Conn
	jsCtx            nats.JetStreamContext
}

func (sw *jsProducerSingleWorker) OnClosed(conn *nats.Conn) error {
	sw.natsProducerConn = nil
	sw.jsCtx = nil

	return nil
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
		sw.logger.Print("subscription: lost NATS origin connection")

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

func (sw *jsProducerSingleWorker) Produce(ctx context.Context, msg *nats.Msg) {
	err := sw.produce(ctx, msg)
	if err != nil {
		sw.logger.Printf("producer: unable to produce nats message - %e", err)
	}
}

func (sw *jsProducerSingleWorker) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	err := sw.produce(ctx, msg)
	if err != nil {
		sw.logger.Printf("producer: unable to produce nats message - %e", err)

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
		sw.logger.Printf("producer: %s - %e",
			"received nil pubAck", ErrNilPubAck)

		return ErrNilPubAck
	}

	return nil
}

func NewJsProducerSingleWorkerService(logger *log.Logger,
	natsProducerConn *nats.Conn,
	streamName string,
	subjects []string,
) *jsProducerSingleWorker {
	workersPool := &jsProducerSingleWorker{
		logger:     logger,
		streamName: streamName,
		subjects:   subjects,

		natsProducerConn: natsProducerConn,
		jsCtx:            nil, // will be filed @ init stage
	}

	return workersPool
}
