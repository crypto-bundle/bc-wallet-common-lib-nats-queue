package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// producerWorkerWrapper ...
type producerWorkerWrapper struct {
	logger           *zap.Logger
	natsProducerConn *nats.Conn
	msgChannel       <-chan *nats.Msg

	subject string
	num     uint16
}

func (ww *producerWorkerWrapper) Run(ctx context.Context) {
	for {
		select {
		case v := <-ww.msgChannel:
			err := ww.publishMsg(v)
			if err != nil {
				ww.logger.Error("send message to broker service failed", zap.Error(err),
					zap.String(QueueSubjectNameTag, v.Subject))
			}

		case <-ctx.Done():
			ww.logger.Info("producer worker. received close worker message")
			return
		}
	}
}

func (ww *producerWorkerWrapper) OnClosed(conn *nats.Conn) error {
	ww.natsProducerConn = nil

	return nil
}

func (ww *producerWorkerWrapper) PublishMsg(v *nats.Msg) error {
	return ww.publishMsg(v)
}

func (ww *producerWorkerWrapper) publishMsg(v *nats.Msg) error {
	err := ww.natsProducerConn.PublishMsg(v)
	if err != nil {
		return err
	}

	return nil
}

func newProducerWorker(logger *zap.Logger,
	workerNum uint16,
	msgChannel chan *nats.Msg,
	subject string,
	natsProducerConn *nats.Conn,
) *producerWorkerWrapper {
	l := logger.Named("producer.service.worker").
		With(zap.String(QueueSubjectNameTag, subject),
			zap.Uint16(WorkerUnitNumberTag, workerNum))

	return &producerWorkerWrapper{
		logger:           l,
		msgChannel:       msgChannel,
		subject:          subject,
		natsProducerConn: natsProducerConn,
		num:              0,
	}
}
