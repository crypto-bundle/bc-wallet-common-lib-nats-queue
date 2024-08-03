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

import "github.com/nats-io/nats.go"

func (c *Connection) NewJsConsumerPushQueueGroupSingeWorker(
	consumerCfg consumerConfigQueueGroup,
	handler consumerHandler,
) *jsConsumerPushQueueGroupSingeWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	requeueDelays := consumerCfg.GetNakDelayTimings()

	ww := &jsConsumerWorkerWrapper{
		msgChannel: nil, // cuz channel-less single-worker worker pool
		logger: c.stdLoggerFactory.WithFields(map[string]interface{}{
			natsFunctionalUnitTag: natsJetStreamConsumerUnitNameTag,
		}),
		handler:           handler,
		reQueueDelay:      requeueDelays,
		reQueueDelayCount: uint64(len(requeueDelays) - 1),
	}

	subscriptionSvc := newJsPushQueueGroupHandlerSubscription(c.stdLoggerFactory, c.originConn, consumerCfg, ww.ProcessMsg)

	workersPool := &jsConsumerPushQueueGroupSingeWorker{
		logger: c.stdLoggerFactory.WithFields(map[string]interface{}{
			natsFunctionalUnitTag: natsWorkerNameTag,
			natsConsumerTypeTag:   natsPushTypeQueueGroupConsumerNameTag,
		}),
		subscriptionSvc: subscriptionSvc,
		worker:          ww,
	}

	c.consumers = append(c.consumers, workersPool)
	c.consumerCounter++

	return workersPool
}

func (c *Connection) NewJsPullTypeConsumerWorkersPool(consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeChannelConsumerWorkerPool {
	c.mu.Lock()
	defer c.mu.Unlock()

	msgChannel := make(chan *nats.Msg, consumerCfg.GetWorkersCount())

	pullSubscriber := newJsPullChanSubscriptionService(c.stdLoggerFactory, c.originConn, consumerCfg, msgChannel)

	workersPool := &jsPullTypeChannelConsumerWorkerPool{
		handler: handler,
		logger: c.stdLoggerFactory.WithFields(map[string]interface{}{
			natsFunctionalUnitTag: natsConsumerWorkerPoolUnitNameTag,
			natsConsumerTypeTag:   natsPullTypeConsumerNameTag,
		}),
		msgChannel:     msgChannel,
		subjectName:    consumerCfg.GetSubjectName(),
		pullSubscriber: pullSubscriber,
	}

	requeueDelays := consumerCfg.GetNakDelayTimings()

	for i := uint32(0); i < consumerCfg.GetWorkersCount(); i++ {
		ww := &jsConsumerWorkerWrapper{
			msgChannel: msgChannel,
			handler:    workersPool.handler,
			logger: c.stdLoggerFactory.WithFields(map[string]interface{}{
				natsFunctionalUnitTag: natsWorkerNameTag,
				natsConsumerTypeTag:   natsPullTypeConsumerNameTag,
				workerUnitNumberTag:   i,
			}),
			reQueueDelay:      requeueDelays,
			reQueueDelayCount: uint64(len(requeueDelays) - 1),
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	c.consumers = append(c.consumers, workersPool)
	c.consumerCounter++

	return workersPool
}

func (c *Connection) NewJsPullTypeConsumerSingleWorker(consumerCfg consumerConfigPullType,
	handler consumerHandler,
) *jsPullTypeHandlerConsumer {
	c.mu.Lock()
	defer c.mu.Unlock()

	jsConsumer := NewJsPullTypeHandlerConsumer(c.stdLoggerFactory, c.originConn,
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

	jsConsumer := NewJsPushTypeChannelGroupConsumerWorkersPool(c.stdLoggerFactory, c.originConn,
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

	jsConsumer := NewJsPushTypeChannelConsumerWorkersPool(c.stdLoggerFactory, c.originConn,
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
