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

	storage  nats.StorageType
	jsInfo   *nats.StreamInfo
	jsConfig *nats.StreamConfig

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
	workersCount uint16,
	streamName string,
	subjects []string,
	storage nats.StorageType,
) (*jsProducerWorkerPool, error) {
	l := logger.Named("producer.service").
		With(zap.String(QueueStreamNameTag, streamName))

	streamChannel := make(chan *nats.Msg, workersCount)

	jsNatsCtx, err := natsProducerConn.JetStream()
	if err != nil {
		return nil, err
	}

	jsConfig := &nats.StreamConfig{
		Name:     streamName,
		Subjects: subjects,
		Storage:  storage,
	}

	workersPool := &jsProducerWorkerPool{
		logger:     l,
		msgChannel: streamChannel,
		jsConfig:   jsConfig,
		streamName: streamName,
		subject:    subjects,
		storage:    storage,

		natsConn:  natsProducerConn,
		jsNatsCtx: jsNatsCtx,

		workersCount: uint32(workersCount),
		rr:           1, // round-robin index
	}

	for i := uint16(0); i < workersCount; i++ {
		ww := newJsProducerWorker(logger, jsNatsCtx, i,
			streamChannel, streamName,
			subjects, make(chan bool))

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool, nil
}
