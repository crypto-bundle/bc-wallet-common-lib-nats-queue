package nats

import (
	"context"
	"go.uber.org/zap"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// simpleProducerWorkerPool is a minimal Worker implementation that simply wraps a
type simpleProducerWorkerPool struct {
	logger *zap.Logger

	msgChannel chan *nats.Msg

	subjectName string
	groupName   string

	natsProducerConn *nats.Conn
	workers          []*producerWorkerWrapper

	workersCount uint32
	rr           uint32 // round-robin index
}

func (wp *simpleProducerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Error("unable to call onClosed in simple producer pool unit", zap.Error(loopErr))

			err = loopErr
		}
		wp.workers[i] = nil
	}

	wp.natsProducerConn = nil

	close(wp.msgChannel)
	wp.msgChannel = nil

	return err
}

func (wp *simpleProducerWorkerPool) OnReconnect(conn *nats.Conn) error {
	return nil
}

func (wp *simpleProducerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (wp *simpleProducerWorkerPool) Init(ctx context.Context) error {

	return nil
}

func (wp *simpleProducerWorkerPool) Run(ctx context.Context) error {
	wp.run(ctx)

	return nil
}

func (wp *simpleProducerWorkerPool) run(ctx context.Context) {
	for i, _ := range wp.workers {
		go wp.workers[i].Run(ctx)
	}
}

func (wp *simpleProducerWorkerPool) Healthcheck(ctx context.Context) bool {
	if !wp.natsProducerConn.IsConnected() {
		wp.logger.Warn("producer lost nats originConn")

		return false
	}

	return true
}

func (wp *simpleProducerWorkerPool) Produce(ctx context.Context, msg *nats.Msg) {
	wp.msgChannel <- msg
}

func (wp *simpleProducerWorkerPool) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	n := atomic.AddUint32(&wp.rr, 1)
	return wp.workers[n%wp.workersCount].PublishMsg(msg)
}

func NewSimpleProducerWorkersPool(logger *zap.Logger,
	natsProducerConn *nats.Conn,
	workersCount uint16,
	subjectName string,
	groupName string,
) *simpleProducerWorkerPool {
	l := logger.Named("producer.service").
		With(zap.String(QueueSubjectNameTag, subjectName))

	msgChannel := make(chan *nats.Msg, workersCount)

	workersPool := &simpleProducerWorkerPool{
		logger: l,

		subjectName: subjectName,
		groupName:   groupName,

		msgChannel:       msgChannel,
		natsProducerConn: natsProducerConn,
		workers:          make([]*producerWorkerWrapper, workersCount),
		workersCount:     uint32(workersCount),
		rr:               1, // round-robin index
	}

	for i := uint16(0); i < workersCount; i++ {
		ww := newProducerWorker(logger, i, msgChannel, subjectName,
			natsProducerConn)

		workersPool.workers[i] = ww
	}

	return workersPool
}
