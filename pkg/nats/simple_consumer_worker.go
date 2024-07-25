package nats

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

// consumerWorkerWrapper ...
type consumerWorkerWrapper struct {
	msgChannel <-chan *nats.Msg

	handler consumerHandler

	logger *log.Logger

	maxRedeliveryCount uint64
}

func (ww *consumerWorkerWrapper) OnClosed(conn *nats.Conn) error {
	ww.msgChannel = nil
	ww.handler = nil

	return nil
}

func (ww *consumerWorkerWrapper) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			ww.logger.Print("consumer: received close worker message")
			return

		case v, ok := <-ww.msgChannel:
			if !ok {
				ww.logger.Print("consumer: nats message channel is closed")
				return
			}

			ww.processMsg(v)
		}
	}
}

func (ww *consumerWorkerWrapper) processMsg(msg *nats.Msg) {
	decisionDirective, err := ww.handler.Process(context.Background(), msg)
	if err != nil {
		ww.logger.Printf("consumer: proccess message ended with error - %e. decision directive - %s",
			err, decisionDirective)
	}

	switch {
	case decisionDirective == DirectiveForPass:
		arrErr := msg.Ack()
		if arrErr != nil {
			ww.logger.Printf("consumer: unable to ACK message - %e", arrErr)
		}

	case decisionDirective == DirectiveForReQueue:
		nakErr := msg.Nak()
		if nakErr != nil {
			ww.logger.Printf("consumer: unable to RE-QUEUE message - %e", nakErr)
		}

	case decisionDirective == DirectiveForReject:
		termErr := msg.Term()
		if termErr != nil {
			ww.logger.Printf("consumer: unable to REJECTION-ACK message - %e", termErr)
		}
	}
}
