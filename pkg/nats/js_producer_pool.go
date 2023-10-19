package nats

import (
	"context"
	"go.uber.org/zap"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// jsProducerWorkerPool is a minimal Worker implementation that simply wraps a
type jsProducerWorkerPool struct {
	logger *zap.Logger

	msgChannel chan *nats.Msg
	streamName string
	subject    []string

	storage nats.StorageType
	jsInfo  *nats.StreamInfo

	natsConn  *nats.Conn
	jsNatsCtx nats.JetStreamContext

	workers      []*jsProducerWorkerWrapper
	workersCount uint32
	rr           uint32 // round-robin index
}

func (wp *jsProducerWorkerPool) OnReconnect(newConn *nats.Conn) error {
	jsNatsCtx, err := newConn.JetStream()
	if err != nil {
		return err
	}

	wp.jsNatsCtx = jsNatsCtx

	wp.natsConn = newConn

	return nil
}

func (wp *jsProducerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (wp *jsProducerWorkerPool) Healthcheck(ctx context.Context) bool {
	if !wp.natsConn.IsConnected() {
		wp.logger.Warn("producer lost nats originConn")

		return false
	}

	return true
}

func (wp *jsProducerWorkerPool) Init(ctx context.Context) error {
	jsNatsCtx, err := wp.natsConn.JetStream()
	if err != nil {
		return err
	}

	wp.jsNatsCtx = jsNatsCtx

	for i := uint32(0); i < wp.workersCount; i++ {
		ww := newJsProducerWorker(wp.logger, wp.jsNatsCtx, i,
			wp.msgChannel, wp.streamName,
			wp.subject, make(chan bool))

		wp.workers = append(wp.workers, ww)
	}

	return nil
}

func (wp *jsProducerWorkerPool) Run(ctx context.Context) error {
	for i, _ := range wp.workers {
		go wp.workers[i].Run()
	}

	return nil
}

func (wp *jsProducerWorkerPool) Shutdown(ctx context.Context) error {
	for _, w := range wp.workers {
		w.Stop()
	}

	return nil
}

func (wp *jsProducerWorkerPool) Produce(ctx context.Context, msg *nats.Msg) {
	wp.msgChannel <- msg
}

func (wp *jsProducerWorkerPool) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	n := atomic.AddUint32(&wp.rr, 1)
	return wp.workers[n%wp.workersCount].PublishMsg(msg)
}

func NewJsProducerWorkersPool(logger *zap.Logger,
	natsProducerConn *nats.Conn,
	workersCount uint32,
	streamName string,
	subjects []string,
) *jsProducerWorkerPool {
	l := logger.Named("producer.service").
		With(zap.String(QueueStreamNameTag, streamName))

	workersPool := &jsProducerWorkerPool{
		logger:     l,
		msgChannel: make(chan *nats.Msg, workersCount),
		streamName: streamName,
		subject:    subjects,

		natsConn:  natsProducerConn,
		jsNatsCtx: nil, // will be filed @ init stage

		workersCount: workersCount,
		rr:           1, // round-robin index
	}

	return workersPool
}
