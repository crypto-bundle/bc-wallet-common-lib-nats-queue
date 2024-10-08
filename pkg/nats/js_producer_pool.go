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
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// jsProducerWorkerPool is a minimal Worker implementation that simply wraps...
type jsProducerWorkerPool struct {
	l *slog.Logger
	e errorFormatterService

	msgChannel chan *nats.Msg

	natsConn  *nats.Conn
	jsNatsCtx nats.JetStreamContext

	workers      []*jsProducerWorkerWrapper
	workersCount uint32
	rr           uint32 // round-robin index
}

func (wp *jsProducerWorkerPool) OnClosed(conn *nats.Conn) error {
	for index := range wp.workers {
		loopErr := wp.workers[index].OnClosed(conn)
		if loopErr != nil {
			wp.l.Error("unable to call onClosed callback in producer worker pool unit", loopErr)

			return loopErr
		}

		wp.workers[index] = nil
	}

	wp.natsConn = nil
	wp.jsNatsCtx = nil
	close(wp.msgChannel)

	return nil
}

func (wp *jsProducerWorkerPool) OnReconnect(newConn *nats.Conn) error {
	jsNatsCtx, err := newConn.JetStream()
	if err != nil {
		return wp.e.ErrorOnly(err, "unable to get JetStream context")
	}

	wp.jsNatsCtx = jsNatsCtx

	wp.natsConn = newConn

	return nil
}

func (wp *jsProducerWorkerPool) OnDisconnect(_ *nats.Conn, _ error) error {
	return nil
}

func (wp *jsProducerWorkerPool) Healthcheck(_ context.Context) bool {
	if !wp.natsConn.IsConnected() {
		wp.l.Warn("lost NATS origin connection")

		return false
	}

	return true
}

func (wp *jsProducerWorkerPool) Init(ctx context.Context) error {
	jsNatsCtx, err := wp.natsConn.JetStream()
	if err != nil {
		return wp.e.ErrorOnly(err, "unable to get JetStream context")
	}

	wp.jsNatsCtx = jsNatsCtx

	for index := range wp.workers {
		loopErr := wp.workers[index].Init(ctx, jsNatsCtx)
		if loopErr != nil {
			return loopErr
		}
	}

	return nil
}

func (wp *jsProducerWorkerPool) Run(ctx context.Context) error {
	for index := range wp.workers {
		go wp.workers[index].Run(ctx)
	}

	return nil
}

func (wp *jsProducerWorkerPool) Produce(ctx context.Context, msg *nats.Msg) {
	wp.msgChannel <- msg
}

func (wp *jsProducerWorkerPool) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	n := atomic.AddUint32(&wp.rr, 1)

	return wp.workers[n%wp.workersCount].PublishMsg(msg)
}

func NewJsProducerWorkersPool(loggerFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
	natsProducerConn *nats.Conn,
	msgChannel chan *nats.Msg,
	workers []*jsProducerWorkerWrapper,
) *jsProducerWorkerPool {
	workersPool := &jsProducerWorkerPool{
		l: loggerFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsFunctionalUnitTag, QueueProcessingUnitTypeWorkerPoolName),
		),
		e: errFormatterSvc,

		msgChannel: msgChannel,

		natsConn:  natsProducerConn,
		jsNatsCtx: nil, // will be filled @ init stage

		workers:      workers,
		workersCount: uint32(len(workers)),
		rr:           roundRobinInitialIndex, // round-robin index
	}

	return workersPool
}
