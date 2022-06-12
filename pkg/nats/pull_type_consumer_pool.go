package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var (
	ErrNilSubscribeInfo = errors.New("receive nil subscribe info")
)

// PullTypeConsumerWorkerPool is a minimal Worker implementation that simply wraps a
type PullTypeConsumerWorkerPool struct {
	msgChannel chan *nats.Msg

	jsInfo         *nats.StreamInfo
	jsConfig       *nats.StreamConfig
	jsConsumerConn nats.JetStreamContext

	subjectName string
	streamName  string
	durable     bool

	ticker         *time.Ticker
	tickerTimeout  time.Duration
	pullSubscriber *nats.Subscription

	handler consumerHandler
	workers []*consumerWorkerWrapper

	logger *zap.Logger
}

func (wp *PullTypeConsumerWorkerPool) Init(ctx context.Context) error {
	//streamInfo, err := wp.getOrCreateStream(ctx)
	//if err != nil {
	//	return err
	//}
	//
	//if streamInfo == nil {
	//	return ErrReturnedNilStreamInfo
	//}

	wp.ticker = time.NewTicker(wp.tickerTimeout)

	consumerInfo, err := wp.getOrCreateSubscriber(ctx)
	if err != nil {
		return err
	}

	if consumerInfo == nil {
		return ErrReturnedNilConsumerInfo
	}

	streamWilldCard := wp.streamName + ".*"

	subs, err := wp.jsConsumerConn.PullSubscribe(streamWilldCard, wp.subjectName)
	if err != nil {
		return err
	}

	if subs == nil {
		return ErrNilSubscribeInfo
	}

	wp.pullSubscriber = subs

	return nil
}

func (wp *PullTypeConsumerWorkerPool) getOrCreateStream(ctx context.Context) (*nats.StreamInfo, error) {
	streamInfo, err := wp.jsConsumerConn.StreamInfo(wp.streamName)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			stream, addStreamErr := wp.jsConsumerConn.AddStream(wp.jsConfig)
			if addStreamErr != nil {
				return nil, addStreamErr
			}

			streamInfo = stream
		}

		return nil, err
	}

	return streamInfo, nil
}

func (wp *PullTypeConsumerWorkerPool) getOrCreateSubscriber(ctx context.Context) (*nats.ConsumerInfo, error) {
	consumerInfo, err := wp.jsConsumerConn.ConsumerInfo(wp.streamName, wp.subjectName)
	if err != nil {
		if errors.Is(err, nats.ErrConsumerNotFound) {
			consumerConfig := &nats.ConsumerConfig{
				Durable:   wp.subjectName,
				AckPolicy: nats.AckExplicitPolicy,
			}

			consumer, addErr := wp.jsConsumerConn.AddConsumer(wp.streamName, consumerConfig)
			if addErr != nil {
				return nil, addErr
			}

			consumerInfo = consumer
		}

		return nil, err
	}

	return consumerInfo, nil
}

func (wp *PullTypeConsumerWorkerPool) Run(ctx context.Context) error {
	for _, w := range wp.workers {
		w.msgChannel = wp.msgChannel

		go w.Start()
	}

	return wp.run()
}

func (wp *PullTypeConsumerWorkerPool) run() error {
	go func() {
		for {
			select {
			case <-wp.ticker.C:
				msgList, fetchErr := wp.pullSubscriber.Fetch(3)
				if fetchErr != nil {
					wp.logger.Error("unable fetch data", zap.Error(fetchErr))
				}

				for i := 0; i != len(msgList); i++ {
					wp.msgChannel <- msgList[i]
				}
			}
		}
	}()

	return nil
}

func (wp *PullTypeConsumerWorkerPool) Shutdown(ctx context.Context) error {
	for _, w := range wp.workers {
		w.Stop()
	}

	wp.ticker.Stop()

	return nil
}

func NewPullTypeConsumerWorkersPool(logger *zap.Logger,
	msgChannel chan *nats.Msg,
	streamName string,
	subjectName string,
	workersCount uint16,
	tickerTimeout time.Duration,
	handler consumerHandler,
	jsNatsConn nats.JetStreamContext,
) *PullTypeConsumerWorkerPool {
	workersPool := &PullTypeConsumerWorkerPool{
		handler:        handler,
		logger:         logger,
		msgChannel:     msgChannel,
		subjectName:    subjectName,
		streamName:     streamName,
		jsConsumerConn: jsNatsConn,
		tickerTimeout:  tickerTimeout,
		ticker:         nil,
	}

	for i := uint16(0); i < workersCount; i++ {
		ww := &consumerWorkerWrapper{
			msgChannel:       workersPool.msgChannel,
			stopWorkerChanel: make(chan bool),
			handler:          workersPool.handler,
			logger:           logger,
		}

		workersPool.workers = append(workersPool.workers, ww)
	}

	return workersPool
}