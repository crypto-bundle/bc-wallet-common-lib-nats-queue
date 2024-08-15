/*
 *
 *
 * MIT NON-AI License
 *
 * Copyright (c) 2022-2024 Aleksei Kotelnikov(gudron2s@gmail.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of the software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions.
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * In addition, the following restrictions apply:
 *
 * 1. The Software and any modifications made to it may not be used for the purpose of training or improving machine learning algorithms,
 * including but not limited to artificial intelligence, natural language processing, or data mining. This condition applies to any derivatives,
 * modifications, or updates based on the Software code. Any usage of the Software in an AI-training dataset is considered a breach of this License.
 *
 * 2. The Software may not be included in any dataset used for training or improving machine learning algorithms,
 * including but not limited to artificial intelligence, natural language processing, or data mining.
 *
 * 3. Any person or organization found to be in violation of these restrictions will be subject to legal action and may be held liable
 * for any damages resulting from such use.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

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

	reQueueDelayCount uint64
	reQueueDelay      []time.Duration
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
			ww.logger.Print("received close worker message")

			return

		case natsMsg, ok := <-ww.msgChannel:
			if !ok {
				ww.logger.Print("nats message channel is closed")

				return
			}

			ww.processMsg(context.Background(), natsMsg)
		}
	}
}

func (ww *jsConsumerWorkerWrapper) ProcessMsg(msg *nats.Msg) {
	ww.processMsg(context.Background(), msg)
}

func (ww *jsConsumerWorkerWrapper) processMsg(ctx context.Context, msg *nats.Msg) {
	msgMetaData, err := msg.Metadata()
	if err != nil {
		ww.logger.Printf("error: unable to read metadata - %e. %s: %s, ",
			err, SubjectTag, msg.Subject)
	}

	decisionDirective, err := ww.handler.Process(ctx, msg)
	if err != nil {
		ww.logger.Printf("error: proccess message ended with error - %e. decision directive - %s",
			err, decisionDirective)
	}

	switch {
	case decisionDirective == DirectiveForPass:
		arrErr := msg.Ack()
		if arrErr != nil {
			ww.logger.Printf("error: unable to ACK message - %e", arrErr)
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
			ww.logger.Printf("error: unable to RE-QUEUE message - %e", nakErr)
		}

	case decisionDirective == DirectiveForReject:
		termErr := msg.Term()
		if termErr != nil {
			ww.logger.Printf("error: unable to REJECTION-ACK message - %e", err)
		}
	}
}
