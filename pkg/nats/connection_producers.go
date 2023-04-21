package nats

import (
	"github.com/nats-io/nats.go"
)

func (c *Connection) NewJsProducerWorkersPool(
	workersCount uint32,
	streamName string,
	subjects []string,
	storageType nats.StorageType,
) *jsProducerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	producer := NewJsProducerWorkersPool(c.logger, c.originConn, workersCount,
		streamName, subjects, storageType)

	c.producers = append(c.producers, producer)
	c.producersCounter++

	return producer
}

func (c *Connection) NewSimpleProducerWorkersPool(
	workersCount uint16,
	subjectName string,
	groupName string,
) *simpleProducerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	producer := NewSimpleProducerWorkersPool(c.logger, c.originConn, workersCount, subjectName, groupName)

	c.producers = append(c.producers, producer)
	c.producersCounter++

	return producer
}
