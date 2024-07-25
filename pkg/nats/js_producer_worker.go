package nats

import (
	"context"
	"errors"
	"log"

	"github.com/nats-io/nats.go"
)

type ProducerWorkerTask func(msg nats.Msg) error

var (
	ErrNilPubAck = errors.New("nil pub ack received")
)

// jsProducerWorkerWrapper ...
type jsProducerWorkerWrapper struct {
	logger     *log.Logger
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
				ww.logger.Printf("producer worker: %s - %e",
					"unable to send message to broker service", err)
			}

		case <-ctx.Done():
			ww.logger.Print("producer worker: received close worker message")
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
		ww.logger.Printf("producer worker: %s - %e",
			"received nil pubAck", ErrNilPubAck)
		return ErrNilPubAck
	}

	return nil
}

func newJsProducerWorker(logger *log.Logger,
	natsProducerConn nats.JetStreamContext,
	workerNum uint32,
	msgChannel chan *nats.Msg,
	streamName string,
	subjects []string,
) *jsProducerWorkerWrapper {
	return &jsProducerWorkerWrapper{
		logger:           logger,
		msgChannel:       msgChannel,
		streamName:       streamName,
		subjects:         subjects,
		natsProducerConn: natsProducerConn,
		num:              uint16(workerNum),
	}
}
