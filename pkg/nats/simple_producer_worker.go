package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"log"
)

// producerWorkerWrapper ...
type producerWorkerWrapper struct {
	logger *log.Logger

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
				ww.logger.Printf("producer pool: send message to broker service failed - %e",
					err)
			}

		case <-ctx.Done():
			ww.logger.Printf("producer worker: received close worker message")
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

func newProducerWorker(logger *log.Logger,
	workerNum uint16,
	msgChannel chan *nats.Msg,
	subject string,
	natsProducerConn *nats.Conn,
) *producerWorkerWrapper {
	return &producerWorkerWrapper{
		logger:           logger,
		msgChannel:       msgChannel,
		subject:          subject,
		natsProducerConn: natsProducerConn,
		num:              workerNum,
	}
}
