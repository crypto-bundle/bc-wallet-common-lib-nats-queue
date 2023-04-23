package nats

import (
	"strings"
	"time"
)

type NatsConfig struct {
	NatsAddresses string `envconfig:"NATS_ADDRESSES" default:"nats://ns-1:4223,nats://ns-2:4224,nats://na-3:4225"`
	NatsUser      string `envconfig:"NATS_USER" default:"nats"`
	NatsPassword  string `envconfig:"NATS_PASSWORD" default:"password"`

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
