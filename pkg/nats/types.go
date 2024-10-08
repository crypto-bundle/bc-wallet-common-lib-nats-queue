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

const enumCastNilResult = "<nil>"

// ConsumerDirective ....
type ConsumerDirective uint8

const (
	DirectiveForRejectName  = "rejected"
	DirectiveForPassName    = "passed"
	DirectiveForReQueueName = "requeue"
)

const (
	DirectiveForReject ConsumerDirective = iota + 1
	DirectiveForPass
	DirectiveForReQueue
)

func (d ConsumerDirective) String() string {
	switch d {
	case DirectiveForReject:
		return DirectiveForRejectName
	case DirectiveForPass:
		return DirectiveForPassName
	case DirectiveForReQueue:
		return DirectiveForReQueueName
	default:
		return enumCastNilResult
	}
}

type QueueType uint

const (
	QueueTypeNonGroup QueueType = iota + 1
	QueueTypeGroup
)

const (
	QueueTypeNonGroupName = "queue_non_group"
	QueueTypeGroupName    = "queue_group"
)

func (d QueueType) String() string {
	switch d {
	case QueueTypeNonGroup:
		return QueueTypeNonGroupName
	case QueueTypeGroup:
		return QueueTypeGroupName
	default:
		return enumCastNilResult
	}
}

type QueueProcessingUnitType uint

const (
	QueueProcessingUnitTypeConnection QueueProcessingUnitType = iota + 1
	QueueProcessingUnitTypeWorker
	QueueProcessingUnitTypeWorkerPool
	QueueProcessingUnitTypeSubscription
	QueueProcessingUnitTypeSingleWorker
)

const (
	QueueProcessingUnitTypeConnectionName   = "connection"
	QueueProcessingUnitTypeWorkerName       = "worker"
	QueueProcessingUnitTypeWorkerPoolName   = "worker_pool"
	QueueProcessingUnitTypeSubscriptionName = "subscription"
	QueueProcessingUnitTypeSingleWorkerName = "single_worker"
)

func (d QueueProcessingUnitType) String() string {
	switch d {
	case QueueProcessingUnitTypeConnection:
		return QueueProcessingUnitTypeConnectionName
	case QueueProcessingUnitTypeWorker:
		return QueueProcessingUnitTypeWorkerName
	case QueueProcessingUnitTypeWorkerPool:
		return QueueProcessingUnitTypeWorkerPoolName
	case QueueProcessingUnitTypeSubscription:
		return QueueProcessingUnitTypeSubscriptionName
	case QueueProcessingUnitTypeSingleWorker:
		return QueueProcessingUnitTypeSingleWorkerName
	default:
		return enumCastNilResult
	}
}

type SubscriptionHandlerType uint8

const (
	SubscriptionHandlerTypeChannel SubscriptionHandlerType = iota + 1
	SubscriptionHandlerTypeCallback
)

const (
	SubscriptionHandlerTypeChannelName  = "channel"
	SubscriptionHandlerTypeCallbackName = "callback"
)

func (d SubscriptionHandlerType) String() string {
	switch d {
	case SubscriptionHandlerTypeChannel:
		return SubscriptionHandlerTypeChannelName
	case SubscriptionHandlerTypeCallback:
		return SubscriptionHandlerTypeCallbackName
	default:
		return enumCastNilResult
	}
}

type SubscriptionType uint8

const (
	SubscriptionTypePull SubscriptionType = iota + 1
	SubscriptionTypePush
)

const (
	SubscriptionTypePullName = "pull"
	SubscriptionTypePushName = "push"
)

func (d SubscriptionType) String() string {
	switch d {
	case SubscriptionTypePull:
		return SubscriptionTypePullName
	case SubscriptionTypePush:
		return SubscriptionTypePushName
	default:
		return enumCastNilResult
	}
}

type QueueEngine uint8

const (
	QueueEngineJetStream QueueEngine = iota + 1
	QueueEngineCore
)

const (
	QueueEngineJetStreamName = "jet_stream"
	QueueEngineCoreName      = "core"
)

func (d QueueEngine) String() string {
	switch d {
	case QueueEngineJetStream:
		return QueueEngineJetStreamName
	case QueueEngineCore:
		return QueueEngineCoreName
	default:
		return enumCastNilResult
	}
}
