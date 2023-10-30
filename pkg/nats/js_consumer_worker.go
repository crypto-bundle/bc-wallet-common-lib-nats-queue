package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// jsConsumerWorkerWrapper ...
type jsConsumerWorkerWrapper struct {
	msgChannel <-chan *nats.Msg

	handler consumerHandler

	logger *zap.Logger

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

func (ww *jsConsumerWorkerWrapper) ProcessMsg(msg *nats.Msg) {
	ww.processMsg(msg)
}

func (ww *jsConsumerWorkerWrapper) processMsg(msg *nats.Msg) {
	msgMetaData, err := msg.Metadata()
	if err != nil {
		ww.logger.Error("unable to get message metadata", zap.Error(err),
			zap.String(SubjectTag, msg.Subject))
	}

	decisionDirective, err := ww.handler.Process(context.Background(), msg)
	switch {
	case decisionDirective == DirectiveForPass:
		arrErr := msg.Ack()
		if arrErr != nil {
			ww.logger.Error("unable to ACK message", zap.Error(arrErr), zap.Any("message", msg))
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
			ww.logger.Error("unable to RE-QUEUE message", zap.Error(nakErr), zap.Any("message", msg))
		}

	case decisionDirective == DirectiveForReject:
		termErr := msg.Term()
		if termErr != nil {
			ww.logger.Error("unable to REJECTION-ACK message", zap.Error(err), zap.Any("message", msg))
		}
	}
}
