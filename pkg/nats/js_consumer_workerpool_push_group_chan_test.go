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
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type handler func(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error)

func (h handler) Process(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error) {
	return h(ctx, msg)
}

func TestJsPushTypeChannelConsumerWorkersPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	conn := NewConnection(ctx, &NatsConfig{
		NatsAddresses:                     os.Getenv("NATS_ADDRESSES"),
		NatsUser:                          "",
		NatsPassword:                      "",
		NatsConnectionRetryOnFailed:       false,
		NatsConnectionRetryCount:          30,
		NatsConnectionRetryTimeout:        time.Second * 15,
		NatsFlushTimeOut:                  time.Second * 15,
		NatsWorkersPerConsumer:            5,
		NatsSubscriptionRetry:             true,
		NatsSubscriptionRetryCount:        3,
		NatsSubscriptionRetryTimeout:      time.Second * 3,
		NatsSubscriptionReDeliveryTimeout: time.Second * 3,

		nastAddresses: nil,
	}, log.Default())

	err := conn.Connect()
	if err != nil {
		t.Fatal("unable to connect nats", err)
	}

	var clb handler = func(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error) {
		return DirectiveForPass, nil
	}

	natsConn := conn.GetConnection()
	jsCxt, _ := natsConn.JetStream()

	_, _ = jsCxt.AddStream(&nats.StreamConfig{
		Name:        "test_nats_queue_lib",
		Description: "",
		Subjects: []string{
			"test_nats_queue_lib.test1",
			"test_nats_queue_lib.test2",
			"test_nats_queue_lib.test3",
		},
	})

	consumer := conn.NewJsPushTypeChannelConsumerWorkersPool(&ConsumerConfigGrouped{
		ConsumerConfig: ConsumerConfig{
			SubjectName:            "test_nats_queue_lib.test1",
			WorkersCount:           3,
			AutoReSubscribeEnabled: true,
			AutoResubscribeCount:   3,
			AutoResubscribeDelay:   3,
			NakDelayTimings:        nil,
			BackOffTimings:         nil,
			MaxDeliveryCount:       0,
		},
		QueueGroupName: "test_nats_queue_lib_test1_grp",
	}, clb)

	err = consumer.Init(ctx)
	if err != nil {
		t.Fatal("unable to init nats consumer")
	}

	err = consumer.Run(ctx)
	if err != nil {
		t.Fatal("unable to start nats consumer", err)
	}

	cancel()

	conn.onClosed(natsConn)

	_ = jsCxt.DeleteStream("test_nats_queue_lib")

	err = conn.Close()
	if err != nil {
		t.Fatal("unable to close nats connection", err)
	}

}
