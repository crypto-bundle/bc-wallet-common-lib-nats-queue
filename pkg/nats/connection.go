package nats

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type Connection struct {
	mu sync.Mutex

	originConn *nats.Conn

	cfg     configParams
	options []nats.Option
	logger  *zap.Logger

	user      string
	password  string
	addresses []string

	retryTimeOut time.Duration
	retryCount   uint16

	consumerCounter uint64
	consumers       []consumerService

	producersCounter uint64
	producers        []producerService
}

// Connect ...
func (c *Connection) Connect() error {
	inst, err := nats.Connect(c.cfg.GetNatsJoinedAddresses(), c.options...)
	if err != nil {
		return err
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

	c.logger.Info("nats connection successfully closed")

	return nil
}

func (c *Connection) onDisconnect(conn *nats.Conn, err error) {
	c.logger.Warn("received on DisconnectErr event - calling OnDisconnect on all consumers/producers",
		zap.Error(err))

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint64(0); i != c.producersCounter; i++ {
		producerErr := c.producers[i].OnDisconnect(conn, err)
		if producerErr != nil {
			c.logger.Warn("unable to call onDisconnect on producer",
				zap.Error(producerErr), zap.Uint64(ProducerIndex, i))
		}
	}

	for i := uint64(0); i != c.consumerCounter; i++ {
		consumerErr := c.consumers[i].OnDisconnect(conn, err)
		if consumerErr != nil {
			c.logger.Warn("unable to call onDisconnect on consumer",
				zap.Error(consumerErr), zap.Uint64(ConsumerIndex, i))
		}
	}
}

func (c *Connection) onClosed(newConn *nats.Conn) {
	c.logger.Warn("received onClosed event - calling OnClosed on all consumers/producers")

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint64(0); i != c.producersCounter; i++ {
		producerErr := c.producers[i].OnClosed(newConn)
		if producerErr != nil {
			c.logger.Warn("unable to call onClosed on producer",
				zap.Error(producerErr), zap.Uint64(ProducerIndex, i))
		}
	}

	for i := uint64(0); i != c.consumerCounter; i++ {
		consumerErr := c.consumers[i].OnClosed(newConn)
		if consumerErr != nil {
			c.logger.Warn("unable to call onClosed on consumer",
				zap.Error(consumerErr), zap.Uint64(ConsumerIndex, i))
		}
	}
}

func (c *Connection) onReconnect(newConn *nats.Conn) {
	c.originConn = newConn

	c.logger.Warn("received on OnReconnect event - calling OnReconnect on all consumers/producers")

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint64(0); i != c.producersCounter; i++ {
		producerErr := c.producers[i].OnReconnect(newConn)
		if producerErr != nil {
			c.logger.Warn("unable to call onReconnect on producer",
				zap.Error(producerErr), zap.Uint64(ProducerIndex, i))
		}
	}

	for i := uint64(0); i != c.consumerCounter; i++ {
		consumerErr := c.consumers[i].OnReconnect(newConn)
		if consumerErr != nil {
			c.logger.Warn("unable to call onReconnect on consumer",
				zap.Error(consumerErr), zap.Uint64(ConsumerIndex, i))
		}
	}
}

// NewConnection nats originConn instance
func NewConnection(ctx context.Context,
	cfg configParams,
	logger *zap.Logger,
) *Connection {
	options := make([]nats.Option, 0)
	if cfg.IsRetryOnConnectionFailed() {
		options = append(options, nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(int(cfg.GetNatsConnectionRetryCount())),
			nats.ReconnectWait(cfg.GetNatsConnectionRetryTimeout()),
		)
	}

	nats.RegisterEncoder(PROTOBUF_ENCODER, &ProtobufEncoder{})

	conn := &Connection{
		logger:     logger,
		originConn: nil, // will be settled @ Connect receiver-function call
		options:    options,

		cfg: cfg,

		retryCount:   cfg.GetNatsConnectionRetryCount(),
		retryTimeOut: cfg.GetNatsConnectionRetryTimeout(),

		consumerCounter:  0,
		producersCounter: 0,
	}

	return conn
}
