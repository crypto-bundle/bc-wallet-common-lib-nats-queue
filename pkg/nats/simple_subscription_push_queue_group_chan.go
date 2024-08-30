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

type simplePushQueueGroupChanSubscription struct {
	natsSubs *nats.Subscription
	natsConn *nats.Conn

	subjectName string
	groupName   string

	autoReSubscribe        bool
	autoReSubscribeCount   uint16
	autoReSubscribeTimeout time.Duration

	msgChannel chan *nats.Msg

	logger *log.Logger
	e      errorFormatterService
}

func (s *simplePushQueueGroupChanSubscription) OnClosed(conn *nats.Conn) error {
	s.natsSubs = nil
	s.natsConn = nil

	close(s.msgChannel)

	return nil
}

func (s *simplePushQueueGroupChanSubscription) OnReconnect(newConn *nats.Conn) error {
	s.natsConn = newConn

	err := s.tryResubscribe()
	if err != nil {
		return err
	}

	return nil
}

func (s *simplePushQueueGroupChanSubscription) OnDisconnect(conn *nats.Conn, err error) error {
	return nil
}

func (s *simplePushQueueGroupChanSubscription) Healthcheck(ctx context.Context) bool {
	if !s.natsConn.IsConnected() {
		s.logger.Print("lost NATS origin connection")

		return false
	}

	if !s.natsSubs.IsValid() {
		s.logger.Print("lost NATS subscription")

		return false
	}

	return true
}

func (s *simplePushQueueGroupChanSubscription) Init(_ context.Context) error {
	return nil
}

func (s *simplePushQueueGroupChanSubscription) Subscribe(_ context.Context) error {
	subs, err := s.natsConn.ChanQueueSubscribe(s.subjectName, s.groupName, s.msgChannel)
	if err != nil {
		return s.e.ErrorOnly(err, "unable to make NATS channel queue-subscription")
	}

	s.natsSubs = subs

	return nil
}

func (s *simplePushQueueGroupChanSubscription) UnSubscribe() error {
	err := s.natsSubs.Drain()
	if err != nil {
		return s.e.ErrorOnly(err, "unable to drain NATS-subscription")
	}

	return nil
}

func (s *simplePushQueueGroupChanSubscription) tryResubscribe() error {
	if !s.autoReSubscribe {
		return nil
	}

	var err error

	for i := uint16(0); i != s.autoReSubscribeCount; i++ {
		subs, subsErr := s.natsConn.ChanQueueSubscribe(s.subjectName, s.groupName, s.msgChannel)
		if subsErr != nil {
			s.logger.Printf("error: unable to re-subscribe - %e. %s: %d",
				subsErr, ResubscribeTag, i)

			err = subsErr

			time.Sleep(s.autoReSubscribeTimeout)

			continue
		}

		s.natsSubs = subs

		s.logger.Print("re-subscription success")

		return nil
	}

	if err != nil {
		return s.e.ErrorOnly(err)
	}

	return nil
}

func newSimplePushQueueGroupSubscriptionService(loggerFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
	natsConn *nats.Conn,
	consumerCfg consumerConfigQueueGroup,
	msgChannel chan *nats.Msg,
) *simplePushQueueGroupChanSubscription {
	return &simplePushQueueGroupChanSubscription{
		natsConn: natsConn,
		natsSubs: nil, // it will be set @ run stage

		subjectName: consumerCfg.GetSubjectName(),
		groupName:   consumerCfg.GetQueueGroupName(),

		autoReSubscribe:        consumerCfg.IsAutoReSubscribeEnabled(),
		autoReSubscribeCount:   consumerCfg.GetAutoResubscribeCount(),
		autoReSubscribeTimeout: consumerCfg.GetAutoResubscribeDelay(),

		msgChannel: msgChannel,
		logger: loggerFactorySvc.WithFields(
			map[string]interface{}{
				natsFunctionalUnitTag: natsSimpleSubscriptionUnitNameTag,
				natsConsumerTypeTag:   natsPushTypeQueueGroupConsumerNameTag,
			}),
		e: errFormatterSvc,
	}
}
