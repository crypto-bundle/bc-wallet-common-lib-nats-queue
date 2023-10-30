package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap/zaptest"
	"os"
	"testing"
	"time"
)

type handler func(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error)

func (h handler) Process(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error) {
	return h(ctx, msg)
}

func TestJsPushTypeChannelConsumerWorkersPool(t *testing.T) {
	logger := zaptest.NewLogger(t)

	ctx, cancel := context.WithCancel(context.Background())

	conn := NewConnection(ctx, &NatsConfig{
		NatsAddresses:                     os.Getenv("NATS_ADDRESSES"),
		NatsUser:                          "",
		NatsPassword:                      "",
		NatsConnectionRetryOnFailed:       false,
		NatsConnectionRetryCount:          30,
		NatsConnectionRetryTimeout:        time.Second * 15,
		NatsFlushTimeOut:                  time.Second * 15,
		NatsWorkersPerConsumer:            5,
		NatsSubscriptionRetry:             true,
		NatsSubscriptionRetryCount:        3,
		NatsSubscriptionRetryTimeout:      time.Second * 3,
		NatsSubscriptionReDeliveryTimeout: time.Second * 3,

		nastAddresses: nil,
	}, logger)

	err := conn.Connect()
	if err != nil {
		t.Fatal("unable to connect nats", err)
	}

	var clb handler = func(ctx context.Context, msg *nats.Msg) (ConsumerDirective, error) {
		return DirectiveForPass, nil
	}

	natsConn := conn.GetConnection()
	jsCxt, _ := natsConn.JetStream()

	_, _ = jsCxt.AddStream(&nats.StreamConfig{
		Name:        "test_nats_queue_lib",
		Description: "",
		Subjects: []string{
			"test_nats_queue_lib.test1",
			"test_nats_queue_lib.test2",
			"test_nats_queue_lib.test3",
		},
	})

	consumer := conn.NewJsPushTypeChannelConsumerWorkersPool(&ConsumerConfigGrouped{
		ConsumerConfig: ConsumerConfig{
			SubjectName:            "test_nats_queue_lib.test1",
			WorkersCount:           3,
			AutoReSubscribeEnabled: true,
			AutoResubscribeCount:   3,
			AutoResubscribeDelay:   3,
			NakDelayTimings:        nil,
			BackOffTimings:         nil,
			MaxDeliveryCount:       0,
		},
		QueueGroupName: "test_nats_queue_lib_test1_grp",
	}, clb)

	err = consumer.Init(ctx)
	if err != nil {
		t.Fatal("unable to init nats consumer")
	}

	err = consumer.Run(ctx)
	if err != nil {
		t.Fatal("unable to start nats consumer", err)
	}

	cancel()

	conn.onClosed(natsConn)

	_ = jsCxt.DeleteStream("test_nats_queue_lib")

	err = conn.Close()
	if err != nil {
		t.Fatal("unable to close nats connection", err)
	}

}
