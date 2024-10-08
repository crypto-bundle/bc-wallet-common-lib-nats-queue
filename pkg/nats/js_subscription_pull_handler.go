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
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

type jsPullHandlerSubscription struct {
	jsNatsCtx              nats.JetStreamContext
	e                      errorFormatterService
	handler                func(msg *nats.Msg)
	natsSubs               *nats.Subscription
	ticker                 *time.Ticker
	l                      *slog.Logger
	natsConn               *nats.Conn
	subjectName            string
	durableName            string
	subscribeNatsOptions   []nats.SubOpt
	autoReSubscribeCount   int
	autoReSubscribeTimeout time.Duration
	fetchInterval          time.Duration
	fetchTimeout           time.Duration
	fetchLimit             uint
	autoReSubscribe        bool
}

func (s *jsPullHandlerSubscription) OnClosed(conn *nats.Conn) error {
	s.natsConn = nil
	s.natsSubs = nil
	s.jsNatsCtx = nil
	s.ticker = nil

	return nil
}

func (s *jsPullHandlerSubscription) OnReconnect(newConn *nats.Conn) error {
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

func (s *jsPullHandlerSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return s.onDisconnect(conn, err)
}

func (s *jsPullHandlerSubscription) Healthcheck(ctx context.Context) bool {
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

func (s *jsPullHandlerSubscription) Init(_ context.Context) error {
	jsNatsCtx, err := s.natsConn.JetStream()
	if err != nil {
		return s.e.ErrorOnly(err, "unable to make NATS jet-stream context")
	}

	s.jsNatsCtx = jsNatsCtx

	return nil
}

func (s *jsPullHandlerSubscription) Subscribe(ctx context.Context) error {
	subs, err := s.jsNatsCtx.PullSubscribe(s.subjectName, s.durableName, s.subscribeNatsOptions...)
	if err != nil {
		return s.e.ErrorOnly(err, "unable to make NATS pull subscription")
	}

	s.natsSubs = subs
	s.ticker = time.NewTicker(s.fetchInterval)

	go s.run(ctx)

	return nil
}

func (s *jsPullHandlerSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return s.e.ErrorOnly(err, "unable to drain NATS-subscription")
	}

	s.ticker.Stop()

	return nil
}

func (s *jsPullHandlerSubscription) run(ctx context.Context) {
	for {
		select {
		case <-s.ticker.C:
			msgList, fetchErr := s.natsSubs.Fetch(int(s.fetchLimit),
				nats.MaxWait(s.fetchTimeout))
			if fetchErr == nil {
				for i := 0; i != len(msgList); i++ {
					s.handler(msgList[i])
				}

				continue
			}

			if errors.Is(fetchErr, nats.ErrTimeout) {
				continue
			}

			s.l.Error("unable fetch data", fetchErr)

		case <-ctx.Done():
			s.l.Info("received close message")

			return
		}
	}
}

func (s *jsPullHandlerSubscription) onDisconnect(_ *nats.Conn, _ error) error {
	s.ticker.Stop()

	return nil
}

func (s *jsPullHandlerSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error

	for i := range s.autoReSubscribeCount {
		subs, subsErr := s.jsNatsCtx.PullSubscribe(s.subjectName, s.durableName, s.subscribeNatsOptions...)
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

//nolint:dupl //it's ok, function does not same with newJsPullChanSubscriptionService
func newJsPullHandlerSubscriptionService(loggerFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
	natsConn *nats.Conn,
	consumerCfg consumerConfigPullType,
	handler func(msg *nats.Msg),
) *jsPullHandlerSubscription {
	subOptions := []nats.SubOpt{
		nats.AckWait(consumerCfg.GetAckWaitTiming()),
	}

	if consumerCfg.GetBackOffTimings() != nil {
		subOptions = append(subOptions,
			nats.BackOff(consumerCfg.GetBackOffTimings()),
			nats.MaxDeliver(consumerCfg.GetMaxDeliveryCount()),
		)
	}

	return &jsPullHandlerSubscription{
		l: loggerFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsQueueEngineTag, QueueEngineJetStreamName),
			slog.String(natsFunctionalUnitTag, QueueProcessingUnitTypeSubscriptionName),
			slog.String(natsSubscriptionQueueType, QueueTypeNonGroupName),
			slog.String(natsSubscriptionType, SubscriptionTypePullName),
			slog.String(natsSubscriptionHandlerType, SubscriptionHandlerTypeCallbackName),
		),
		e: errFormatterSvc,

		natsConn:  natsConn,
		jsNatsCtx: nil, // it will be set @ init stage
		natsSubs:  nil, // it will be set @ run stage

		subjectName: consumerCfg.GetSubjectName(),
		durableName: consumerCfg.GetDurableName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),
		subscribeNatsOptions:   subOptions,

		fetchInterval: consumerCfg.GetFetchInterval(),
		fetchTimeout:  consumerCfg.GetFetchTimeout(),
		fetchLimit:    consumerCfg.GetFetchLimit(),

		handler: handler,

		ticker: nil, // it will be set @ Subscribe stage
	}
}
