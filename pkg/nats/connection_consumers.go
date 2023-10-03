package nats

import (
	"github.com/nats-io/nats.go"
	"time"
)

func (c *Connection) NewJsConsumerPushQueueGroupSingeWorker(
	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
	subOpt ...nats.SubOpt,
) *jsConsumerPushQueueGroupSingeWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsConsumerPushQueueGroupSingeWorker(c.logger, c.originConn, subjectName,
		queueGroupName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		handler, subOpt...)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewJsPullTypeConsumerWorkersPool(workersCount uint16,
	subjectName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	fetchInterval time.Duration,
	fetchTimeout time.Duration,
	fetchLimit uint,

	handler consumerHandler,
	subOpt ...nats.SubOpt,
) *jsPullTypeChannelConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPullTypeConsumerWorkersPool(c.logger, c.originConn, workersCount,
		subjectName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		fetchInterval, fetchTimeout, fetchLimit,
		handler, subOpt...)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewJsPushTypeChannelConsumerWorkersPool(workersCount uint16,
	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
	subOpt ...nats.SubOpt,
) *jsPushTypeChannelConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPushTypeChannelConsumerWorkersPool(c.logger, c.originConn, workersCount,
		subjectName, queueGroupName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		handler, subOpt...)

	c.consumers = append(c.consumers, jsConsumer)
	c.consumerCounter++

	return jsConsumer
}

func (c *Connection) NewSimpleConsumerWorkersPool(workersCount uint16,
	subjectName string,
	groupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
) *simpleConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	simpleConsumer := NewSimpleConsumerWorkersPool(c.logger, c.originConn, workersCount,
		subjectName, groupName, autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		handler)

	c.consumers = append(c.consumers, simpleConsumer)
	c.consumerCounter++

	return simpleConsumer
}
