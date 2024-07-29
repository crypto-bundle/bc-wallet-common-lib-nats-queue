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

	"github.com/nats-io/nats.go"
)

// jsPullTypeHandlerConsumer is a minimal Worker implementation that simply wraps a
type jsPullTypeHandlerConsumer struct {
	pullSubscriber subscriptionService

	worker *jsConsumerWorkerWrapper

	logger *log.Logger
}

func (wp *jsPullTypeHandlerConsumer) OnClosed(conn *nats.Conn) error {
	var err error

	err = wp.pullSubscriber.OnClosed(conn)
	if err != nil {
		wp.logger.Printf("consumer: unable to call onClosed in pull-type subscription service - %e", err)
	}

	wp.pullSubscriber = nil

	return err
}

func (wp *jsPullTypeHandlerConsumer) OnReconnect(conn *nats.Conn) error {
	err := wp.pullSubscriber.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeHandlerConsumer) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.pullSubscriber.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsPullTypeHandlerConsumer) Healthcheck(ctx context.Context) bool {
	return wp.pullSubscriber.Healthcheck(ctx)
}

func (wp *jsPullTypeHandlerConsumer) Init(ctx context.Context) error {
	err := wp.pullSubscriber.Init(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeHandlerConsumer) Run(ctx context.Context) error {
	err := wp.pullSubscriber.Subscribe(ctx)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()

		err = wp.pullSubscriber.UnSubscribe()
		if err != nil {
			wp.logger.Printf("consumer: unable to unSubscribe - %e", err)
		}
	}()

	return nil
}

func NewJsPullTypeHandlerConsumer(loggerFactorySvc loggerService,
	jsNatsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeHandlerConsumer {

	requeueDelays := consumerCfg.GetNakDelayTimings()

	ww := &jsConsumerWorkerWrapper{
		msgChannel: nil, // cuz channel-less single-worker worker pool
		logger: loggerFactorySvc.WithFields(map[string]interface{}{
			natsFunctionalUnitTag: natsJetStreamConsumerUnitNameTag,
		}),
		handler:           handler,
		reQueueDelay:      requeueDelays,
		reQueueDelayCount: uint64(len(requeueDelays) - 1),
	}

	pullSubscriber := newJsPullHandlerSubscriptionService(loggerFactorySvc.WithFields(
		map[string]interface{}{
			natsFunctionalUnitTag: natsJetStreamSubscriptionUnitNameTag,
			natsConsumerTypeTag:   natsPullTypeQueueGroupConsumerNameTag,
		}), jsNatsConn,
		consumerCfg, ww.ProcessMsg)

	return &jsPullTypeHandlerConsumer{
		pullSubscriber: pullSubscriber,
		worker:         ww,
		logger: loggerFactorySvc.WithFields(map[string]interface{}{
			natsFunctionalUnitTag: natsWorkerNameTag,
			natsConsumerTypeTag:   natsPullTypeQueueGroupConsumerNameTag,
		}),
	}
}
