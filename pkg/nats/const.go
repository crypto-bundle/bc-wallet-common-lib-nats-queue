package nats

const (
	SubjectTag     = "subject"
	DeliveredCount = "delivered_count"

	ResubscribeTag = "resubscribe_attempt"

	ConsumerIndex = "consumer_index"
	ProducerIndex = "producer_index"

	natsFunctionalUnitTag = "nats_unit"
	natsConsumerTypeTag   = "consumer_type"

	natsConnectionUnitNameTag                  = "connection"
	natsJetStreamConsumerUnitNameTag           = "js_consumer"
	natsJetStreamSubscriptionUnitNameTag       = "js_subscription"
	natsJetStreamConsumerWorkerPoolUnitNameTag = "js_worker_pool"
	natsSubscriptionUnitWorkerNameTag          = "js_worker"

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
