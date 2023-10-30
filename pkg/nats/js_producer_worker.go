package nats

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type ProducerWorkerTask func(msg nats.Msg) error

var (
	ErrNilPubAck = errors.New("nil pub ack received")
)

// jsProducerWorkerWrapper ...
type jsProducerWorkerWrapper struct {
	logger     *zap.Logger
	msgChannel <-chan *nats.Msg
	jsInfo     *nats.StreamInfo

	streamName string
	subjects   []string

	natsProducerConn nats.JetStreamContext

	num uint16
}

func (ww *jsProducerWorkerWrapper) OnClosed(conn *nats.Conn) error {
	ww.natsProducerConn = nil
	ww.jsInfo = nil

	return nil
}

func (ww *jsProducerWorkerWrapper) Run(ctx context.Context) {
	for {
		select {
		case v := <-ww.msgChannel:
			err := ww.publishMsg(v)
			if err != nil {
				ww.logger.Error("send message to broker service failed", zap.Error(err),
					zap.String(QueueSubjectNameTag, v.Subject),
					zap.String(QueueStreamNameTag, ww.jsInfo.Config.Name))
			}

		case <-ctx.Done():
			ww.logger.Info("producer worker. received close worker message")
			return
		}
	}
}

func (ww *jsProducerWorkerWrapper) PublishMsg(v *nats.Msg) error {
	return ww.publishMsg(v)
}

func (ww *jsProducerWorkerWrapper) publishMsg(v *nats.Msg) error {
	pubAck, err := ww.natsProducerConn.PublishMsg(v)
	if err != nil {
		return err
	}

	if pubAck == nil {
		ww.logger.Error("received nil pubAck", zap.Error(ErrNilPubAck))
		return ErrNilPubAck
	}

	return nil
}

func newJsProducerWorker(logger *zap.Logger,
	natsProducerConn nats.JetStreamContext,
	workerNum uint32,
	msgChannel chan *nats.Msg,
	streamName string,
	subjects []string,
) *jsProducerWorkerWrapper {
	l := logger.Named("producer.service.worker").
		With(zap.Uint32(WorkerUnitNumberTag, workerNum))

	return &jsProducerWorkerWrapper{
		logger:           l,
		msgChannel:       msgChannel,
		streamName:       streamName,
		subjects:         subjects,
		natsProducerConn: natsProducerConn,
		num:              0,
	}
}
