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

type consumerHandler interface {
	Process(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error)
}

type subscriptionService interface {
	Healthcheck(ctx context.Context) bool
	Init(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type consumerWorker interface {
	Run(ctx context.Context) error
	ProcessMsg(msg *nats.Msg)
}
