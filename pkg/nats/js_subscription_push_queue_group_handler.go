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
	"time"

	"github.com/nats-io/nats.go"
)

type jsPushQueueGroupHandlerSubscription struct {
	natsSubs  *nats.Subscription
	natsConn  *nats.Conn
	jsNatsCtx nats.JetStreamContext

	subjectName    string
	queueGroupName string

	handler func(msg *nats.Msg)

	l *slog.Logger
	e errorFormatterService

	subscribeNatsOptions []nats.SubOpt

	autoReSubscribeTimeout time.Duration
	autoReSubscribeCount   int
	autoReSubscribe        bool
}

func (s *jsPushQueueGroupHandlerSubscription) OnReconnect(newConn *nats.Conn) error {
	jsNatsCtx, err := newConn.JetStream()
	if err != nil {
		return s.e.ErrorOnly(err, "unable to make NATS jet-stream context")
	}

	s.jsNatsCtx = jsNatsCtx

	s.natsConn = newConn

	err = s.tryResubscribe()
	if err != nil {
		return err
	}

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) OnClosed(_ *nats.Conn) error {
	s.natsSubs = nil
	s.jsNatsCtx = nil
	s.natsConn = nil
	s.handler = nil

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) Healthcheck(ctx context.Context) bool {
	if !s.natsConn.IsConnected() {
		s.l.Warn("lost NATS origin connection")

		return false
	}

	if !s.natsSubs.IsValid() {
		s.l.Warn("lost NATS subscription")

		return false
	}

	return true
}

func (s *jsPushQueueGroupHandlerSubscription) Init(ctx context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return s.e.ErrorOnly(err, "unable to make NATS jet-stream context")
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.QueueSubscribe(s.subjectName, s.queueGroupName,
		s.handler, s.subscribeNatsOptions...)
	if err != nil {
		return s.e.ErrorOnly(err, "unable to make NATS queue subscription")
	}

	s.natsSubs = subs

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return s.e.ErrorOnly(err, "unable to drain NATS-subscription")
	}

	return nil
}

func (s *jsPushQueueGroupHandlerSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error = nil

	for i := range s.autoReSubscribeCount {
		subs, subsErr := s.jsNatsCtx.QueueSubscribe(s.subjectName, s.queueGroupName,
			s.handler, s.subscribeNatsOptions...)
		if subsErr != nil {
			s.l.Error("unable to re-subscribe", subsErr,
				slog.Int(ResubscribeTag, i))

			err = subsErr

			time.Sleep(s.autoReSubscribeTimeout)

			continue
		}

		s.natsSubs = subs

		s.l.Info("re-subscription success")

		return nil
	}

	if err != nil {
		return s.e.ErrorOnly(err)
	}

	return nil
}

func newJsPushQueueGroupHandlerSubscription(loggerFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,

	handler func(msg *nats.Msg),
) *jsPushQueueGroupHandlerSubscription {
	subOptions := []nats.SubOpt{
		nats.AckWait(consumerCfg.GetAckWaitTiming()),
	}

	if consumerCfg.GetBackOffTimings() != nil {
		subOptions = append(subOptions,
			nats.BackOff(consumerCfg.GetBackOffTimings()),
			nats.MaxDeliver(consumerCfg.GetMaxDeliveryCount()),
		)
	}

	return &jsPushQueueGroupHandlerSubscription{
		natsConn:  natsConn,
		natsSubs:  nil, // it will be set @ Subscribe stage
		jsNatsCtx: nil, // it will be set @ init stage

		subjectName:    consumerCfg.GetSubjectName(),
		queueGroupName: consumerCfg.GetQueueGroupName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),
		subscribeNatsOptions:   subOptions,

		handler: handler,
		l: loggerFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsQueueEngineTag, QueueEngineJetStreamName),
			slog.String(natsFunctionalUnitTag, QueueProcessingUnitTypeSubscriptionName),
			slog.String(natsSubscriptionQueueType, QueueTypeGroupName),
			slog.String(natsSubscriptionType, SubscriptionTypePushName),
			slog.String(natsSubscriptionHandlerType, SubscriptionHandlerTypeCallbackName),
		),
		e: errFormatterSvc,
	}
}
