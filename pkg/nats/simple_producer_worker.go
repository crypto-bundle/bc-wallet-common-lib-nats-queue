package nats

import (
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// producerWorkerWrapper ...
type producerWorkerWrapper struct {
	logger           *zap.Logger
	natsProducerConn *nats.Conn
	msgChannel       <-chan *nats.Msg

	closeChanel chan bool

	subject string
	num     uint16
}

func (ww *producerWorkerWrapper) Run() {
	for {
		select {
		case v := <-ww.msgChannel:
			err := ww.publishMsg(v)
			if err != nil {
				ww.logger.Error("send message to broker service failed", zap.Error(err),
					zap.String(QueueSubjectNameTag, v.Subject))
			}

		case <-ww.closeChanel:
			ww.logger.Info("producer worker. received close worker message")
			return
		}
	}
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

func (ww *producerWorkerWrapper) Stop() {
	ww.closeChanel <- true
}

func newProducerWorker(logger *zap.Logger,
	workerNum uint16,
	msgChannel chan *nats.Msg,
	subject string,
	natsProducerConn *nats.Conn,
	closeChan chan bool,
) *producerWorkerWrapper {
	l := logger.Named("producer.service.worker").
		With(zap.String(QueueSubjectNameTag, subject),
			zap.Uint16(WorkerUnitNumberTag, workerNum))

	return &producerWorkerWrapper{
		logger:           l,
		msgChannel:       msgChannel,
		subject:          subject,
		natsProducerConn: natsProducerConn,
		closeChanel:      closeChan,
		num:              0,
	}
}
