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
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type Connection struct {
	mu sync.Mutex

	originConn *nats.Conn

	cfg     configParams
	options []nats.Option

	logFactory loggerService

	l *slog.Logger
	e errorFormatterService

	addresses []string

	retryTimeOut time.Duration
	retryCount   uint16

	consumerCounter uint
	consumers       []consumerService

	producersCounter uint
	producers        []producerService
}

func (c *Connection) IsHealed(ctx context.Context) bool {
	status := c.originConn.Status()
	switch status {
	case nats.DISCONNECTED, nats.CLOSED:
		return false
	default:
		return c.healthCheckConsumers(ctx) && c.healthCheckProducers(ctx)
	}
}

func (c *Connection) healthCheckConsumers(ctx context.Context) bool {
	isHealed := true
	for i := uint(0); i != c.consumerCounter; i++ {
		isHealed = isHealed && c.consumers[i].Healthcheck(ctx)
	}

	return isHealed
}

func (c *Connection) healthCheckProducers(ctx context.Context) bool {
	isHealed := true
	for i := uint(0); i != c.producersCounter; i++ {
		isHealed = isHealed && c.producers[i].Healthcheck(ctx)
	}

	return isHealed
}

// Connect ...
func (c *Connection) Connect() error {
	inst, err := nats.Connect(c.cfg.GetNatsJoinedAddresses(), c.options...)
	if err != nil {
		return c.e.ErrorOnly(err, "unable connect to nats server")
	}

	inst.SetDisconnectErrHandler(c.onDisconnect)
	inst.SetClosedHandler(c.onClosed)
	inst.SetReconnectHandler(c.onReconnect)

	c.originConn = inst

	return nil
}

// GetConnection ...
func (c *Connection) GetConnection() *nats.Conn {
	return c.originConn
}

func (c *Connection) Close() error {
	c.originConn.Close()

	c.l.Info("nats connection successfully closed")

	return nil
}

func (c *Connection) onDisconnect(conn *nats.Conn, err error) {
	c.l.Error("received on DisconnectErr event - calling OnDisconnect on all consumers/producers", err)

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint(0); i != c.producersCounter; i++ {
		producerErr := c.producers[i].OnDisconnect(conn, err)
		if producerErr != nil {
			c.l.Error("unable to call onDisconnect on producer", producerErr,
				slog.Uint64(ProducerIndex, uint64(i)))
		}
	}

	for i := uint(0); i != c.consumerCounter; i++ {
		consumerErr := c.consumers[i].OnDisconnect(conn, err)
		if consumerErr != nil {
			c.l.Error("unable to call onDisconnect on consumer", consumerErr,
				slog.Uint64(ConsumerIndex, uint64(i)))
		}
	}
}

func (c *Connection) onClosed(newConn *nats.Conn) {
	c.l.Error("received onClosed event - calling OnClosed on all consumers/producers")

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint(0); i != c.producersCounter; i++ {
		producerErr := c.producers[i].OnClosed(newConn)
		if producerErr != nil {
			c.l.Error("unable to call onClosed on producer", producerErr,
				slog.Uint64(ProducerIndex, uint64(i)))
		}
	}

	for i := uint(0); i != c.consumerCounter; i++ {
		consumerErr := c.consumers[i].OnClosed(newConn)
		if consumerErr != nil {
			c.l.Error("unable to call onClosed on consumer", consumerErr,
				slog.Uint64(ConsumerIndex, uint64(i)))
		}
	}
}

func (c *Connection) onReconnect(newConn *nats.Conn) {
	c.originConn = newConn

	c.l.Error("received on OnReconnect event - calling OnReconnect on all consumers/producers")

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint(0); i != c.producersCounter; i++ {
		producerErr := c.producers[i].OnReconnect(newConn)
		if producerErr != nil {
			c.l.Error("unable to call onReconnect on producer", producerErr,
				slog.Uint64(ProducerIndex, uint64(i)))
		}
	}

	for i := uint(0); i != c.consumerCounter; i++ {
		consumerErr := c.consumers[i].OnReconnect(newConn)
		if consumerErr != nil {
			c.l.Error("unable to call onReconnect on consumer", consumerErr,
				slog.Uint64(ConsumerIndex, uint64(i)))
		}
	}
}

// NewConnection nats originConn instance
func NewConnection(cfg configParams,
	logFactorySvc loggerService,
	errFormatterSvc errorFormatterService,
) *Connection {
	options := make([]nats.Option, 0)
	if cfg.IsRetryOnConnectionFailed() {
		options = append(options, nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(int(cfg.GetNatsConnectionRetryCount())),
			nats.ReconnectWait(cfg.GetNatsConnectionRetryTimeout()),
		)
	}

	nats.RegisterEncoder(ProtobufEncoderName, &ProtobufEncoder{})

	conn := &Connection{
		mu: sync.Mutex{},

		logFactory: logFactorySvc,
		l: logFactorySvc.NewSlogLoggerEntryWithFields(
			slog.String(natsFunctionalUnitTag, natsConnectionUnitNameTag),
		),
		e:          errFormatterSvc,
		originConn: nil, // will be settled @ Connect receiver-function call
		options:    options,

		cfg: cfg,

		addresses: cfg.GetNatsAddresses(),

		retryCount:   cfg.GetNatsConnectionRetryCount(),
		retryTimeOut: cfg.GetNatsConnectionRetryTimeout(),

		consumerCounter: 0,
		consumers:       nil,

		producersCounter: 0,
		producers:        nil,
	}

	return conn
}
