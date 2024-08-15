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
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

// simpleProducerWorkerPool is a minimal Worker implementation that simply wraps a
type simpleProducerWorkerPool struct {
	logger *log.Logger

	msgChannel chan *nats.Msg

	natsProducerConn *nats.Conn
	workers          []*producerWorkerWrapper

	workersCount uint32
	rr           uint32 // round-robin index
}

func (wp *simpleProducerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Printf("error: unable to call onClosed in simple producer pool unit - %e",
				loopErr)

			err = loopErr
		}

		wp.workers[i] = nil
	}

	wp.natsProducerConn = nil

	close(wp.msgChannel)
	wp.msgChannel = nil

	return err
}

func (wp *simpleProducerWorkerPool) OnReconnect(conn *nats.Conn) error {
	return nil
}

func (wp *simpleProducerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (wp *simpleProducerWorkerPool) Init(ctx context.Context) error {

	return nil
}

func (wp *simpleProducerWorkerPool) Run(ctx context.Context) error {
	wp.run(ctx)

	return nil
}

func (wp *simpleProducerWorkerPool) run(ctx context.Context) {
	for i, _ := range wp.workers {
		go wp.workers[i].Run(ctx)
	}
}

func (wp *simpleProducerWorkerPool) Healthcheck(ctx context.Context) bool {
	if !wp.natsProducerConn.IsConnected() {
		wp.logger.Print("lost NATS origin connection")

		return false
	}

	return true
}

func (wp *simpleProducerWorkerPool) Produce(ctx context.Context, msg *nats.Msg) {
	wp.msgChannel <- msg
}

func (wp *simpleProducerWorkerPool) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	n := atomic.AddUint32(&wp.rr, 1)

	return wp.workers[n%wp.workersCount].PublishMsg(msg)
}

func NewSimpleProducerWorkersPool(loggerFactorySvc loggerService,
	natsProducerConn *nats.Conn,
	msgChannel chan *nats.Msg,
	workers []*producerWorkerWrapper,
) *simpleProducerWorkerPool {
	logger := loggerFactorySvc.WithFields(map[string]interface{}{
		natsFunctionalUnitTag: natsProducerWorkerPoolUnitNameTag,
	})

	workersPool := &simpleProducerWorkerPool{
		logger: logger,

		msgChannel:       msgChannel,
		natsProducerConn: natsProducerConn,
		workers:          workers,
		workersCount:     uint32(len(workers)),
		rr:               1, // round-robin index
	}

	return workersPool
}
