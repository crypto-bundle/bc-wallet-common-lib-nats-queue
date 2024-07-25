package nats

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// jsConsumerWorkerWrapper ...
type jsConsumerWorkerWrapper struct {
	msgChannel <-chan *nats.Msg

	handler consumerHandler

	logger *log.Logger

	maxRedeliveryCount uint64
	reQueueDelayCount  uint64
	reQueueDelay       []time.Duration
}

func (ww *jsConsumerWorkerWrapper) OnClosed(conn *nats.Conn) error {
	ww.msgChannel = nil
	ww.handler = nil

	return nil
}

func (ww *jsConsumerWorkerWrapper) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			ww.logger.Print("consumer worker: received close worker message")
			return

		case v, ok := <-ww.msgChannel:
			if !ok {
				ww.logger.Print("consumer worker: nats message channel is closed")
				return
			}

			ww.processMsg(v)
		}
	}
}

func (ww *jsConsumerWorkerWrapper) ProcessMsg(msg *nats.Msg) {
	ww.processMsg(msg)
}

func (ww *jsConsumerWorkerWrapper) processMsg(msg *nats.Msg) {
	msgMetaData, err := msg.Metadata()
	if err != nil {
		ww.logger.Printf("consumer: %s: %s, error: %e",
			SubjectTag, msg.Subject, err)
	}

	decisionDirective, err := ww.handler.Process(context.Background(), msg)
	if err != nil {
		ww.logger.Printf("consumer: proccess message ended with error - %e. decision directive - %s",
			err, decisionDirective)
	}

	switch {
	case decisionDirective == DirectiveForPass:
		arrErr := msg.Ack()
		if arrErr != nil {
			ww.logger.Printf("consumer worker: unable to ACK message - error: %e", arrErr)
		}

	case decisionDirective == DirectiveForReQueue:
		var delay time.Duration
		if msgMetaData.NumDelivered > ww.reQueueDelayCount {
			delay = ww.reQueueDelay[ww.reQueueDelayCount]
		} else {
			delay = ww.reQueueDelay[msgMetaData.NumDelivered-1]
		}

		nakErr := msg.NakWithDelay(delay)
		if nakErr != nil {
			ww.logger.Printf("consumer worker: unable to RE-QUEUE message - error: %e", nakErr)
		}

	case decisionDirective == DirectiveForReject:
		termErr := msg.Term()
		if termErr != nil {
			ww.logger.Printf("consumer worker: unable to REJECTION-ACK message - error: %e", err)
		}
	}
}
