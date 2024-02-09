package nats

const (
	QueueStreamNameTag  = "queue_stream"
	QueueSubjectNameTag = "queue_subject"

	QueuePubAckStreamNameTag = "queue_pub_ack_stream"
	QueuePubAckSequenceTag   = "queue_pub_ack_sequence"

	WorkerUnitNumberTag = "worker_unit_num"
)

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
	return [...]string{"",
		DirectiveForRejectName,
		DirectiveForPassName,
		DirectiveForReQueueName,
	}[d]
}

type ConsumerType uint

const (
	ConsumerTypeJSPushGroupSingleWorker ConsumerType = iota + 1
	ConsumerTypeJSPushGroupChanelWorkerPool
	ConsumerTypeJSPullChanelWorkerPool
	ConsumerTypeSimpleGroupChannelWorkerPool
	ConsumerTypeSimpleGroupChannelSingleWorker
)
