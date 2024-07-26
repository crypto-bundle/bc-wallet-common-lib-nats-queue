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

const (
	SubjectTag     = "subject"
	DeliveredCount = "delivered_count"

	ResubscribeTag = "resubscribe_attempt"

	ConsumerIndex = "consumer_index"
	ProducerIndex = "producer_index"

	natsFunctionalUnitTag = "nats_unit"
	natsConsumerTypeTag   = "consumer_type"

	natsConnectionUnitNameTag            = "connection"
	natsJetStreamConsumerUnitNameTag     = "js_consumer"
	natsJetStreamProducerUnitNameTag     = "js_producer"
	natsJetStreamSubscriptionUnitNameTag = "js_subscription"
	natsConsumerWorkerPoolUnitNameTag    = "consumer_worker_pool"
	natsProducerWorkerPoolUnitNameTag    = "producer_worker_pool"
	natsWorkerNameTag                    = "js_worker"

	natsPushTypeQueueGroupConsumerNameTag = "push_type_queue_group"
	natsPullTypeQueueGroupConsumerNameTag = "pull_type_queue_group"
	natsPushTypeConsumerNameTag           = "push_type"
	natsPullTypeConsumerNameTag           = "pull_type"

	QueueStreamNameTag  = "queue_stream"
	QueueSubjectNameTag = "queue_subject"

	QueuePubAckStreamNameTag = "queue_pub_ack_stream"
	QueuePubAckSequenceTag   = "queue_pub_ack_sequence"

	workerUnitNumberTag = "worker_unit_num"
)
