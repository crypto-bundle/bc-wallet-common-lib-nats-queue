package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// consumerWorkerWrapper ...
type consumerWorkerWrapper struct {
	msgChannel <-chan *nats.Msg

	handler consumerHandler

	logger *zap.Logger

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
			ww.logger.Info("consumer worker. received close worker message")
			return

		case v, ok := <-ww.msgChannel:
			if !ok {
				ww.logger.Warn("consumer worker. nats message channel is closed")
				return
			}

			ww.processMsg(v)
		}
	}
}

func (ww *consumerWorkerWrapper) processMsg(msg *nats.Msg) {
	decisionDirective, err := ww.handler.Process(context.Background(), msg)
	switch {
	case decisionDirective == DirectiveForPass:
		arrErr := msg.Ack()
		if arrErr != nil {
			ww.logger.Error("unable to ACK message", zap.Error(arrErr), zap.Any("message", msg))
		}

	case decisionDirective == DirectiveForReQueue:
		nakErr := msg.Nak()
		if nakErr != nil {
			ww.logger.Error("unable to RE-QUEUE message", zap.Error(nakErr), zap.Any("message", msg))
		}

	case decisionDirective == DirectiveForReject:
		termErr := msg.Term()
		if termErr != nil {
			ww.logger.Error("unable to REJECTION-ACK message", zap.Error(err), zap.Any("message", msg))
		}
	}
}
