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

// jsProducerSingleWorker ...
type jsProducerSingleWorker struct {
	logger *log.Logger

	streamName string
	subjects   []string

	natsProducerConn *nats.Conn
	jsCtx            nats.JetStreamContext
}

func (sw *jsProducerSingleWorker) OnClosed(_ *nats.Conn) error {
	sw.natsProducerConn = nil
	sw.jsCtx = nil

	return nil
}

func (sw *jsProducerSingleWorker) OnReconnect(newConn *nats.Conn) error {
	sw.natsProducerConn = newConn

	jsNatsCtx, err := newConn.JetStream()
	if err != nil {
		return err
	}

	sw.jsCtx = jsNatsCtx

	return nil
}

func (sw *jsProducerSingleWorker) OnDisconnect(
	_ *nats.Conn,
	_ error,
) error {
	return nil
}

func (sw *jsProducerSingleWorker) Healthcheck(_ context.Context) bool {
	if !sw.natsProducerConn.IsConnected() {
		sw.logger.Print("lost NATS origin connection")

		return false
	}

	return true
}

func (sw *jsProducerSingleWorker) Init(_ context.Context) error {
	jsNatsCtx, err := sw.natsProducerConn.JetStream()
	if err != nil {
		return err
	}

	sw.jsCtx = jsNatsCtx

	return nil
}

func (sw *jsProducerSingleWorker) Run(_ context.Context) error {
	return nil
}

func (sw *jsProducerSingleWorker) Produce(ctx context.Context, msg *nats.Msg) {
	err := sw.produce(ctx, msg)
	if err != nil {
		sw.logger.Printf("error: unable to produce nats message - %e", err)
	}
}

func (sw *jsProducerSingleWorker) ProduceSync(ctx context.Context, msg *nats.Msg) error {
	err := sw.produce(ctx, msg)
	if err != nil {
		sw.logger.Printf("error: unable to produce nats message - %e", err)

		return err
	}

	return nil
}

func (sw *jsProducerSingleWorker) produce(_ context.Context, msg *nats.Msg) error {
	pubAck, err := sw.jsCtx.PublishMsg(msg)
	if err != nil {
		return err
	}

	if pubAck == nil {
		sw.logger.Printf("error: %s - %e",
			"received nil pubAck", ErrNilPubAck)

		return ErrNilPubAck
	}

	return nil
}

func NewJsProducerSingleWorkerService(loggerFactorySvc loggerService,
	natsProducerConn *nats.Conn,
	jsProducerCtx nats.JetStreamContext,
	streamName string,
	subjects []string,
) *jsProducerSingleWorker {
	workersPool := &jsProducerSingleWorker{
		logger: loggerFactorySvc.WithFields(
			map[string]interface{}{
				natsFunctionalUnitTag: natsJetStreamProducerUnitNameTag,
			}),
		streamName:       streamName,
		subjects:         subjects,
		natsProducerConn: natsProducerConn,
		jsCtx:            jsProducerCtx, // will be filed @ init stage
	}

	return workersPool
}
