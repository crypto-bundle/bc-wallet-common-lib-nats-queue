package nats

import (
	"sync/atomic"
	"time"
)

func (c *Connection) NewJsConsumerPushQueueGroupSingeWorker(
	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
) *jsConsumerPushQueueGroupSingeWorker {
	jsConsumer := NewJsConsumerPushQueueGroupSingeWorker(c.logger, c.originConn, subjectName,
		queueGroupName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		handler)

	c.consumers[atomic.AddInt64(&c.consumerCounter, 1)] = jsConsumer

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
) *jsPullTypeChannelConsumerWorkerPool {
	jsConsumer := NewJsPullTypeConsumerWorkersPool(c.logger, c.originConn, workersCount,
		subjectName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		fetchInterval, fetchTimeout, fetchLimit,
		handler)

	c.consumers[atomic.AddInt64(&c.consumerCounter, 1)] = jsConsumer

	return jsConsumer
}

func (c *Connection) NewJsPushTypeChannelConsumerWorkersPool(workersCount uint16,
	subjectName string,
	queueGroupName string,

	autoReSubscribe bool,
	autoReSubscribeCount uint16,
	autoReSubscribeTimeout time.Duration,

	handler consumerHandler,
) *jsPushTypeChannelConsumerWorkerPool {
	jsConsumer := NewJsPushTypeChannelConsumerWorkersPool(c.logger, c.originConn, workersCount,
		subjectName, queueGroupName,
		autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		handler)

	c.consumers[atomic.AddInt64(&c.consumerCounter, 1)] = jsConsumer

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
	simpleConsumer := NewSimpleConsumerWorkersPool(c.logger, c.originConn, workersCount,
		subjectName, groupName, autoReSubscribe, autoReSubscribeCount, autoReSubscribeTimeout,
		handler)

	c.consumers[atomic.AddInt64(&c.consumerCounter, 1)] = simpleConsumer

	return simpleConsumer
}
