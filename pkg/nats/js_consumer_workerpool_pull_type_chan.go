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

// jsPullTypeChannelConsumerWorkerPool is a minimal Worker implementation that simply wraps...
type jsPullTypeChannelConsumerWorkerPool struct {
	l *slog.Logger
	e errorFormatterService

	msgChannel chan *nats.Msg

	subjectName string

	pullSubscriber subscriptionService
	handler        consumerHandler
	workers        []*jsConsumerWorkerWrapper
}

func (wp *jsPullTypeChannelConsumerWorkerPool) OnClosed(conn *nats.Conn) error {
	var err error

	for index := range wp.workers {
		loopErr := wp.workers[index].OnClosed(conn)
		if loopErr != nil {
			wp.l.Error("unable to call onClosed callback in consumer worker pool unit",
				loopErr)
		}

		wp.workers[index] = nil
	}

	wp.handler = nil
	close(wp.msgChannel)

	err = wp.pullSubscriber.OnClosed(conn)
	if err != nil {
		wp.l.Error("unable to call onClosed callback in subscriber service", err)
	}

	close(wp.msgChannel)
	wp.handler = nil
	wp.pullSubscriber = nil

	return err
}

func (wp *jsPullTypeChannelConsumerWorkerPool) OnReconnect(conn *nats.Conn) error {
	err := wp.pullSubscriber.OnReconnect(conn)
	if err != nil {
		return err
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) OnDisconnect(conn *nats.Conn, err error) error {
	retErr := wp.pullSubscriber.OnDisconnect(conn, err)
	if retErr != nil {
		return retErr
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Healthcheck(ctx context.Context) bool {
	return wp.pullSubscriber.Healthcheck(ctx)
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Init(ctx context.Context) error {
	err := wp.pullSubscriber.Init(ctx)
	if err != nil {
		return wp.e.ErrorOnly(err)
	}

	return nil
}

func (wp *jsPullTypeChannelConsumerWorkerPool) Run(ctx context.Context) error {
	for _, w := range wp.workers {
		w.msgChannel = wp.msgChannel

		go w.Run(ctx)
	}

	err := wp.pullSubscriber.Subscribe(ctx)
	if err != nil {
		return wp.e.ErrorOnly(err)
	}

	go func() {
		<-ctx.Done()

		err = wp.pullSubscriber.UnSubscribe()
		if err != nil {
			wp.l.Error("unable to unSubscribe", err)
		}
	}()

	return nil
}

func NewJsPullTypeConsumerWorkersPool(logFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
	natsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeChannelConsumerWorkerPool {
	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	pullSubscriber := newJsPullChanSubscriptionService(logFactorySvc, errFormatterSvc,
		natsConn, consumerCfg, msgChannel)

	workersPool := &jsPullTypeChannelConsumerWorkerPool{
		e: errFormatterSvc,
		l: logFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsFunctionalUnitTag, natsConsumerWorkerPoolUnitNameTag),
			slog.String(natsConsumerTypeTag, natsPullTypeConsumerNameTag),
		),
		handler:        handler,
		msgChannel:     msgChannel,
		subjectName:    consumerCfg.GetSubjectName(),
		pullSubscriber: pullSubscriber,
		workers:        nil,
	}

	requeueDelays := consumerCfg.GetNakDelayTimings()

	for index := range consumerCfg.GetWorkersCount() {
		workerWrapper := &jsConsumerWorkerWrapper{
			msgChannel: msgChannel,
			handler:    workersPool.handler,
			l: logFactorySvc.NewSlogLoggerEntryWithFields(
				slog.String(natsFunctionalUnitTag, natsWorkerNameTag),
				slog.String(natsConsumerTypeTag, natsPullTypeConsumerNameTag),
				slog.Int(workerUnitNumberTag, int(index)),
			),
			reQueueDelay:      requeueDelays,
			reQueueDelayCount: uint64(len(requeueDelays) - 1),
		}

		workersPool.workers = append(workersPool.workers, workerWrapper)
	}

	return workersPool
}
