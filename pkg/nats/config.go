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
	"strings"
	"time"
)

const (
	DefaultAckWaitTiming = time.Second * 8
)

type NatsConfig struct {
	nastAddresses                     []string
	NatsAddresses                     string        `envconfig:"NATS_ADDRESSES" default:"nats://ns-1:4223,nats://ns-2:4224,nats://na-3:4225"`
	NatsUser                          string        `envconfig:"NATS_USER" required:"true" secret:"true"`
	NatsPassword                      string        `envconfig:"NATS_PASSWORD" required:"true" secret:"true"`
	NatsConnectionRetryTimeout        time.Duration `envconfig:"NATS_CONNECTION_RETRY_TIMEOUT" default:"15s"`
	NatsFlushTimeOut                  time.Duration `envconfig:"NATS_FLUSH_TIMEOUT" default:"15s"`
	NatsSubscriptionRetryTimeout      time.Duration `envconfig:"NATS_SUBSCRIPTION_RETRY_TIMEOUT" default:"3s"`
	NatsSubscriptionReDeliveryTimeout time.Duration `envconfig:"NATS_SUBSCRIPTION_REDELIVERY_TIMEOUT" default:"3s"`
	NatsConnectionRetryCount          uint16        `envconfig:"NATS_CONNECTION_RETRY_COUNT" default:"30"`
	NatsWorkersPerConsumer            uint16        `envconfig:"NATS_WORKER_PER_CONSUMER" default:"5"`
	NatsSubscriptionRetryCount        uint16        `envconfig:"NATS_SUBSCRIPTION_RETRY_COUNT" default:"3"`
	NatsConnectionRetryOnFailed       bool          `envconfig:"NATS_CONNECTION_RETRY" default:"true"`
	NatsSubscriptionRetry             bool          `envconfig:"NATS_SUBSCRIPTION_RETRY" default:"true"`
}

func (c *NatsConfig) GetNatsAddresses() []string {
	return c.nastAddresses
}

func (c *NatsConfig) GetNatsJoinedAddresses() string {
	return c.NatsAddresses
}

// func (c *NatsConfig) GetNatsHost() string {
//	return c.NatsHost
// }
//
// func (c *NatsConfig) GetNatsPort() uint16 {
//	return c.NatsPort
// }

func (c *NatsConfig) GetNatsUser() string {
	return c.NatsUser
}

func (c *NatsConfig) GetNatsPassword() string {
	return c.NatsPassword
}

func (c *NatsConfig) IsRetryOnConnectionFailed() bool {
	return c.NatsConnectionRetryOnFailed
}

func (c *NatsConfig) GetNatsConnectionRetryCount() uint16 {
	return c.NatsConnectionRetryCount
}

func (c *NatsConfig) GetNatsConnectionRetryTimeout() time.Duration {
	return c.NatsConnectionRetryTimeout
}

func (c *NatsConfig) GetFlushTimeout() time.Duration {
	return c.NatsFlushTimeOut
}

func (c *NatsConfig) GetWorkersCountPerConsumer() uint16 {
	return c.NatsWorkersPerConsumer
}

// Prepare variables to static configuration...
func (c *NatsConfig) Prepare() error {
	endpoints := strings.Split(c.NatsAddresses, ",")
	length := len(endpoints)

	if length < 1 {
		return nil
	}

	c.nastAddresses = endpoints

	return nil
}

func (c *NatsConfig) PrepareWith(
	_ ...interface{}, // dependencies service-components for configuration
) error {
	return nil
}

type ConsumerConfig struct {
	SubjectName string

	NakDelayTimings  []time.Duration
	BackOffTimings   []time.Duration
	MaxDeliveryCount int
	AckWaitTiming    time.Duration

	WorkersCount           uint32
	AutoResubscribeCount   uint32
	AutoResubscribeDelay   time.Duration
	AutoReSubscribeEnabled bool
}

func (c *ConsumerConfig) GetSubjectName() string {
	return c.SubjectName
}

func (c *ConsumerConfig) IsAutoReSubscribeEnabled() bool {
	return c.AutoReSubscribeEnabled
}

func (c *ConsumerConfig) GetAutoResubscribeCount() int {
	return int(c.AutoResubscribeCount)
}

func (c *ConsumerConfig) GetAutoResubscribeDelay() time.Duration {
	return c.AutoResubscribeDelay
}

func (c *ConsumerConfig) GetBackOffTimings() []time.Duration {
	return c.BackOffTimings
}

func (c *ConsumerConfig) GetMaxDeliveryCount() int {
	return c.MaxDeliveryCount
}

func (c *ConsumerConfig) GetNakDelayTimings() []time.Duration {
	return c.NakDelayTimings
}

func (c *ConsumerConfig) GetAckWaitTiming() time.Duration {
	if c.AckWaitTiming == 0 {
		return DefaultAckWaitTiming
	}

	return c.AckWaitTiming
}

func (c *ConsumerConfig) GetWorkersCount() uint32 {
	return c.WorkersCount
}

type ConsumerConfigGrouped struct {
	ConsumerConfig
	QueueGroupName string
}

func (c *ConsumerConfigGrouped) GetQueueGroupName() string {
	return c.QueueGroupName
}

type ConsumerConfigPullType struct {
	ConsumerConfig

	DurableName string

	FetchInterval time.Duration
	FetchTimeout  time.Duration
	FetchLimit    uint
}

func (c *ConsumerConfigPullType) GetDurableName() string {
	return c.DurableName
}

func (c *ConsumerConfigPullType) GetFetchInterval() time.Duration {
	return c.FetchInterval
}

func (c *ConsumerConfigPullType) GetFetchTimeout() time.Duration {
	return c.FetchTimeout
}

func (c *ConsumerConfigPullType) GetFetchLimit() uint {
	return c.FetchLimit
}
