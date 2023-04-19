package nats

import (
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

func (c *Connection) NewJsProducerWorkersPool(
	workersCount uint16,
	streamName string,
	subjects []string,
	storageType nats.StorageType,
) (*jsProducerWorkerPool, error) {
	producer, err := NewJsProducerWorkersPool(c.logger, c.originConn, workersCount,
		streamName, subjects, storageType)
	if err != nil {
		return nil, err
	}

	c.producers[atomic.AddInt64(&c.producersCounter, 1)] = producer

	return producer, nil
}

func (c *Connection) NewSimpleProducerWorkersPool(
	workersCount uint16,
	subjectName string,
	groupName string,
) (*simpleProducerWorkerPool, error) {
	producer, err := NewSimpleProducerWorkersPool(c.logger, c.originConn, workersCount, subjectName, groupName)
	if err != nil {
		return nil, err
	}

	c.producers[atomic.AddInt64(&c.producersCounter, 1)] = producer

	return producer, nil
}
