package nats

func (c *Connection) NewJsProducerSingleWorker(
	streamName string,
	subjects []string,
) *jsProducerSingleWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	producer := NewJsProducerSingleWorkerService(c.logger, c.originConn,
		streamName, subjects)

	c.producers = append(c.producers, producer)
	c.producersCounter++

	return producer
}

func (c *Connection) NewJsProducerWorkersPool(
	workersCount uint32,
	streamName string,
	subjects []string,
) *jsProducerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	producer := NewJsProducerWorkersPool(c.logger, c.originConn, workersCount,
		streamName, subjects)

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
