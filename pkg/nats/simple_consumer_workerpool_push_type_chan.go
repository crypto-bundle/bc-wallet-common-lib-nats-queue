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
	"github.com/nats-io/nats.go"
	"log"
)

// simpleConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type simpleConsumerWorkerPool struct {
	handler consumerHandler
	workers []*consumerWorkerWrapper

	subscriptionSrv subscriptionService

	msgChannel chan *nats.Msg

	logger *log.Logger
}

func (wp *simpleConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for i, _ := range wp.workers {
		loopErr := wp.workers[i].OnClosed(conn)
		if loopErr != nil {
			wp.logger.Printf("consumer: unable to call onClosed in simple producer pool unit - %e", loopErr)

			err = loopErr
		}

		wp.workers[i] = nil
	}

	close(wp.msgChannel)
	wp.msgChannel = nil

	return err
}

func (wp *simpleConsumerWorkerPool) OnReconnect(conn *nats.Conn) error {
	retErr := wp.subscriptionSrv.OnReconnect(conn)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *simpleConsumerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.subscriptionSrv.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *simpleConsumerWorkerPool) Healthcheck(ctx context.Context) bool {
	return wp.subscriptionSrv.Healthcheck(ctx)
}

func (wp *simpleConsumerWorkerPool) Init(ctx context.Context) error {
	return wp.subscriptionSrv.Init(ctx)
}

func (wp *simpleConsumerWorkerPool) Run(ctx context.Context) error {
	for _, w := range wp.workers {
		go w.Run(ctx)
	}

	err := wp.subscriptionSrv.Subscribe(ctx)
	if err != nil {
		wp.logger.Printf("error: unable to subscribe - %e", err)
	}

	go func() {
		<-ctx.Done()

		err = wp.subscriptionSrv.UnSubscribe()
		if err != nil {
			if err != nil {
				wp.logger.Printf("error: unable to unSubscribe - %e", err)
			}
		}

		wp.logger.Printf("successfully unSubscribed")
	}()

	return nil
}

func NewSimpleConsumerWorkersPool(loggerFactorySvc loggerService,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *simpleConsumerWorkerPool {
	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	subscriptionSrv := newSimplePushQueueGroupSubscriptionService(loggerFactorySvc, natsConn,
		consumerCfg, msgChannel)

	workersPool := &simpleConsumerWorkerPool{
		handler: handler,
		logger: loggerFactorySvc.WithFields(map[string]interface{}{
			natsFunctionalUnitTag: natsConsumerWorkerPoolUnitNameTag,
			natsConsumerTypeTag:   natsPushTypeQueueGroupConsumerNameTag,
		}),

		subscriptionSrv: subscriptionSrv,

		msgChannel: msgChannel,
	}

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &consumerWorkerWrapper{
			msgChannel: msgChannel,
			handler:    workersPool.handler,
			logger: loggerFactorySvc.WithFields(map[string]interface{}{
				natsFunctionalUnitTag: natsWorkerNameTag,
				natsConsumerTypeTag:   natsPushTypeQueueGroupConsumerNameTag,
				workerUnitNumberTag:   i,
			}),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}
