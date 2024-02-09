package nats

func (c *Connection) NewJsConsumerPushQueueGroupSingeWorker(
	consumerConfig consumerConfigQueueGroup,
	handler consumerHandler,
) *jsConsumerPushQueueGroupSingeWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsConsumerPushQueueGroupSingeWorker(c.logger, c.originConn, consumerConfig, handler)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewJsPullTypeConsumerWorkersPool(consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeChannelConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPullTypeConsumerWorkersPool(c.logger, c.originConn,
		consumerCfg, handler)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewJsPullTypeConsumerSingleWorker(consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeHandlerConsumer {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPullTypeHandlerConsumer(c.logger, c.originConn,
		consumerCfg, handler)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewJsPushTypeChannelConsumerGroupWorkersPool(consumerConfig consumerConfigQueueGroup,
	handler consumerHandler,
) *jsPushTypeQueueGroupChannelConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPushTypeChannelGroupConsumerWorkersPool(c.logger, c.originConn,
		consumerConfig, handler)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewJsPushTypeChannelConsumerWorkersPool(consumerConfig consumerConfigQueueGroup,
	handler consumerHandler,
) *jsPushTypeChannelConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPushTypeChannelConsumerWorkersPool(c.logger, c.originConn,
		consumerConfig, handler)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewSimpleConsumerWorkersPool(consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *simpleConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	simpleConsumer := NewSimpleConsumerWorkersPool(c.logger, c.originConn, consumerCfg, handler)

	c.consumers = append(c.consumers, simpleConsumer)
	c.consumerCounter++

	return simpleConsumer
}

func (c *Connection) NewSimpleConsumerSingleWorker(consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *simpleConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	simpleConsumer := NewSimpleConsumerWorkersPool(c.logger, c.originConn, consumerCfg, handler)

	c.consumers = append(c.consumers, simpleConsumer)
	c.consumerCounter++

	return simpleConsumer
}
