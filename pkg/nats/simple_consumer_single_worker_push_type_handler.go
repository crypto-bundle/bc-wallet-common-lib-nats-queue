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
	"log/slog"

	"github.com/nats-io/nats.go"
)

// simpleConsumerSingeWorker is a minimal Worker implementation that simply wraps...
type simpleConsumerSingeWorker struct {
	l *slog.Logger
	e errorFormatterService

	subscriptionSvc subscriptionService
	worker          *consumerWorkerWrapper
}

func (wp *simpleConsumerSingeWorker) OnReconnect(conn *nats.Conn) error {
	err := wp.subscriptionSvc.OnReconnect(conn)
	if err != nil {
		return wp.e.ErrorNoWrap(err)
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSvc.OnDisconnect(conn, err)
	if retErr != nil {
		return wp.e.ErrorNoWrap(retErr)
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) OnClosed(conn *nats.Conn) error {
	defer func() {
		wp.subscriptionSvc = nil
	}()

	err := wp.subscriptionSvc.OnClosed(conn)
	if err != nil {
		wp.l.Error("unable to call onClosed callback", err)

		return wp.e.ErrorNoWrap(err)
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) Init(ctx context.Context) error {
	err := wp.subscriptionSvc.Init(ctx)
	if err != nil {
		return wp.e.ErrorNoWrap(err)
	}

	return nil
}

func (wp *simpleConsumerSingeWorker) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSvc.Healthcheck(ctx)
}

func (wp *simpleConsumerSingeWorker) Run(ctx context.Context) error {
	err := wp.subscriptionSvc.Subscribe(ctx)
	if err != nil {
		return wp.e.ErrorNoWrap(err)
	}

	return nil
}

func NewSimpleConsumerSingeWorker(loggerFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *simpleConsumerSingeWorker {
	workerWrapper := &consumerWorkerWrapper{
		l: loggerFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsFunctionalUnitTag, QueueProcessingUnitTypeWorkerName),
		),
		msgChannel: nil, // cuz channel-less single-worker worker pool
		handler:    handler,
	}

	subscriptionSvc := newSimplePushSubscriptionService(loggerFactorySvc, errFormatterSvc, natsConn,
		consumerCfg, workerWrapper.ProcessMsg)

	worker := &simpleConsumerSingeWorker{
		l: loggerFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsFunctionalUnitTag, QueueProcessingUnitTypeSingleWorkerName),
		),
		e:               errFormatterSvc,
		subscriptionSvc: subscriptionSvc,
		worker:          workerWrapper,
	}

	return worker
}
