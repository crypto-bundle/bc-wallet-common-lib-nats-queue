package nats

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
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

	consumerCounter int64
	consumers       []consumerService

	producersCounter int64
	producers        []producerService
}

// Connect ...
func (c *Connection) Connect() error {
	inst, err := nats.Connect(c.cfg.GetNatsJoinedAddresses(), c.options...)
	if err != nil {
		return err
	}

	inst.SetDisconnectErrHandler(c.onDisconnect)
	inst.SetReconnectHandler(c.onReconnect)

	return nil
}

// GetConnection ...
func (c *Connection) GetConnection() *nats.Conn {
	return c.originConn
}

func (c *Connection) Close() error {
	c.originConn.Close()
	return nil
}

func (c *Connection) onDisconnect(conn *nats.Conn, err error) {
	c.logger.Warn("received on DisconnectErr event - calling OnDisconnect on all consumers/producers",
		zap.Error(err))

	for i := int64(0); i != atomic.LoadInt64(&c.producersCounter); i++ {
		producerErr := c.producers[i].OnDisconnect(conn, err)
		if producerErr != nil {
			c.logger.Warn("unable to call onDisconnect on producer",
				zap.Error(producerErr), zap.Int64(ProducerIndex, i))
		}
	}

	for i := int64(0); i != atomic.LoadInt64(&c.consumerCounter); i++ {
		consumerErr := c.consumers[i].OnDisconnect(conn, err)
		if consumerErr != nil {
			c.logger.Warn("unable to call onDisconnect on consumer",
				zap.Error(consumerErr), zap.Int64(ConsumerIndex, i))
		}
	}
}

func (c *Connection) onReconnect(newConn *nats.Conn) {
	c.originConn = newConn

	c.logger.Warn("received on OnReconnect event - calling OnReconnect on all consumers/producers")

	for i := int64(0); i != atomic.LoadInt64(&c.producersCounter); i++ {
		producerErr := c.producers[i].OnReconnect(newConn)
		if producerErr != nil {
			c.logger.Warn("unable to call onDisconnect on producer",
				zap.Error(producerErr), zap.Int64(ProducerIndex, i))
		}
	}

	for i := int64(0); i != atomic.LoadInt64(&c.consumerCounter); i++ {
		consumerErr := c.consumers[i].OnReconnect(newConn)
		if consumerErr != nil {
			c.logger.Warn("unable to call onDisconnect on consumer",
				zap.Error(consumerErr), zap.Int64(ConsumerIndex, i))
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

		retryCount:      cfg.GetNatsConnectionRetryCount(),
		retryTimeOut:    cfg.GetNatsConnectionRetryTimeout(),
		consumerCounter: -1,
	}

	return conn
}
