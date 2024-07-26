package nats

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
