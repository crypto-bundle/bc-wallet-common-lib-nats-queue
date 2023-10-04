package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type configParams interface {
	GetNatsAddresses() []string
	GetNatsJoinedAddresses() string
	GetNatsUser() string
	GetNatsPassword() string
	IsRetryOnConnectionFailed() bool
	GetNatsConnectionRetryCount() uint16
	GetNatsConnectionRetryTimeout() time.Duration
	GetFlushTimeout() time.Duration
	GetWorkersCountPerConsumer() uint16
}

type consumerConfig interface {
	GetWorkersCount() uint32

	GetSubjectName() string

	IsAutoReSubscribeEnabled() bool
	GetAutoResubscribeCount() uint16
	GetAutoResubscribeDelay() time.Duration
	GetNakDelay() time.Duration
	GetBackOff() []time.Duration
}

type consumerConfigQueueGroup interface {
	consumerConfig

	GetQueueGroupName() string
}

type consumerConfigPullType interface {
	consumerConfig

	GetFetchInterval() time.Duration
	GetFetchTimeout() time.Duration
	GetFetchLimit() uint
}

type consumerHandler interface {
	Process(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error)
}

type subscriptionService interface {
	Healthcheck(ctx context.Context) bool
	OnDisconnect(conn *nats.Conn, err error) error
	OnReconnect(conn *nats.Conn) error

	Init(ctx context.Context) error
	Subscribe(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type consumerService interface {
	Healthcheck(ctx context.Context) bool
	OnDisconnect(conn *nats.Conn, err error) error
	OnReconnect(conn *nats.Conn) error

	Init(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type consumerWorker interface {
	Run(ctx context.Context) error
	ProcessMsg(msg *nats.Msg)
}

type producerService interface {
	Healthcheck(ctx context.Context) bool
	OnDisconnect(conn *nats.Conn, err error) error
	OnReconnect(conn *nats.Conn) error

	Init(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error

	Produce(ctx context.Context, msg *nats.Msg)
	ProduceSync(ctx context.Context, msg *nats.Msg) error
}
