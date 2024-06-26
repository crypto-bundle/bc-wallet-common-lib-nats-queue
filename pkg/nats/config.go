package nats

import (
	"strings"
	"time"
)

const (
	DefaultAckWaitTiming = time.Second * 8
)

type NatsConfig struct {
	NatsAddresses string `envconfig:"NATS_ADDRESSES" default:"nats://ns-1:4223,nats://ns-2:4224,nats://na-3:4225"`
	NatsUser      string `envconfig:"NATS_USER" required:"true" secret:"true"`
	NatsPassword  string `envconfig:"NATS_PASSWORD" required:"true" secret:"true"`

	NatsConnectionRetryOnFailed bool          `envconfig:"NATS_CONNECTION_RETRY" default:"true"`
	NatsConnectionRetryCount    uint16        `envconfig:"NATS_CONNECTION_RETRY_COUNT" default:"30"`
	NatsConnectionRetryTimeout  time.Duration `envconfig:"NATS_CONNECTION_RETRY_TIMEOUT" default:"15s"`

	NatsFlushTimeOut time.Duration `envconfig:"NATS_FLUSH_TIMEOUT" default:"15s"`

	NatsWorkersPerConsumer uint16 `envconfig:"NATS_WORKER_PER_CONSUMER" default:"5"`

	// NatsSubscriptionRetry - config option for enable re-subscription of consumer
	NatsSubscriptionRetry bool `envconfig:"NATS_SUBSCRIPTION_RETRY" default:"true"`
	// NatsSubscriptionRetryCount - config option for limiting re-subscription count
	NatsSubscriptionRetryCount uint16 `envconfig:"NATS_SUBSCRIPTION_RETRY_COUNT" default:"3"`
	// NatsSubscriptionRetryTimeout - config option for sets timeout between re-subscription tries
	NatsSubscriptionRetryTimeout time.Duration `envconfig:"NATS_SUBSCRIPTION_RETRY_TIMEOUT" default:"3s"`

	// NatsSubscriptionReDeliveryTimeout - config option for sett nats.AckWait on consumer subscription level
	NatsSubscriptionReDeliveryTimeout time.Duration `envconfig:"NATS_SUBSCRIPTION_REDELIVERY_TIMEOUT" default:"3s"`

	nastAddresses []string
}

func (c *NatsConfig) GetNatsAddresses() []string {
	return c.nastAddresses
}

func (c *NatsConfig) GetNatsJoinedAddresses() string {
	return c.NatsAddresses
}

// func (c *NatsConfig) GetNatsHost() string {
//	return c.NatsHost
// }
//
// func (c *NatsConfig) GetNatsPort() uint16 {
//	return c.NatsPort
// }

func (c *NatsConfig) GetNatsUser() string {
	return c.NatsUser
}

func (c *NatsConfig) GetNatsPassword() string {
	return c.NatsPassword
}

func (c *NatsConfig) IsRetryOnConnectionFailed() bool {
	return c.NatsConnectionRetryOnFailed
}

func (c *NatsConfig) GetNatsConnectionRetryCount() uint16 {
	return c.NatsConnectionRetryCount
}

func (c *NatsConfig) GetNatsConnectionRetryTimeout() time.Duration {
	return c.NatsConnectionRetryTimeout
}

func (c *NatsConfig) GetFlushTimeout() time.Duration {
	return c.NatsFlushTimeOut
}

func (c *NatsConfig) GetWorkersCountPerConsumer() uint16 {
	return c.NatsWorkersPerConsumer
}

// Prepare variables to static configuration
func (c *NatsConfig) Prepare() error {
	endpoints := strings.Split(c.NatsAddresses, ",")
	length := len(endpoints)
	if length < 1 {
		return nil
	}

	c.nastAddresses = endpoints

	return nil
}

func (c *NatsConfig) PrepareWith(dependenciesCfgSrvList ...interface{}) error {
	return nil
}

type ConsumerConfig struct {
	SubjectName string

	WorkersCount uint32

	AutoReSubscribeEnabled bool
	AutoResubscribeCount   uint16
	AutoResubscribeDelay   time.Duration

	NakDelayTimings  []time.Duration
	BackOffTimings   []time.Duration
	MaxDeliveryCount int
	AckWaitTiming    time.Duration
}

func (c *ConsumerConfig) GetSubjectName() string {
	return c.SubjectName
}

func (c *ConsumerConfig) IsAutoReSubscribeEnabled() bool {
	return c.AutoReSubscribeEnabled
}

func (c *ConsumerConfig) GetAutoResubscribeCount() uint16 {
	return c.AutoResubscribeCount
}

func (c *ConsumerConfig) GetAutoResubscribeDelay() time.Duration {
	return c.AutoResubscribeDelay
}

func (c *ConsumerConfig) GetBackOffTimings() []time.Duration {
	return c.BackOffTimings
}

func (c *ConsumerConfig) GetMaxDeliveryCount() int {
	return c.MaxDeliveryCount
}

func (c *ConsumerConfig) GetNakDelayTimings() []time.Duration {
	return c.NakDelayTimings
}

func (c *ConsumerConfig) GetAckWaitTiming() time.Duration {
	if c.AckWaitTiming == 0 {
		return DefaultAckWaitTiming
	}

	return c.AckWaitTiming
}

func (c *ConsumerConfig) GetWorkersCount() uint32 {
	return c.WorkersCount
}

type ConsumerConfigGrouped struct {
	ConsumerConfig
	QueueGroupName string
}

func (c *ConsumerConfigGrouped) GetQueueGroupName() string {
	return c.QueueGroupName
}

type ConsumerConfigPullType struct {
	ConsumerConfig

	DurableName string

	FetchInterval time.Duration
	FetchTimeout  time.Duration
	FetchLimit    uint
}

func (c *ConsumerConfigPullType) GetDurableName() string {
	return c.DurableName
}

func (c *ConsumerConfigPullType) GetFetchInterval() time.Duration {
	return c.FetchInterval
}

func (c *ConsumerConfigPullType) GetFetchTimeout() time.Duration {
	return c.FetchTimeout
}

func (c *ConsumerConfigPullType) GetFetchLimit() uint {
	return c.FetchLimit
}
