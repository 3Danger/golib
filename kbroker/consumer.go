package kbroker

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	Consumer interface {
		Consume(timeout ...time.Duration) (value []byte, err error)
		Commit() error
		Close() error
	}
	consumer struct {
		consumer         *kafka.Consumer
		offsetController OffsetController
	}
)

func (c *consumer) Consume(timeout ...time.Duration) (value []byte, err error) {
	if len(timeout) == 0 {
		timeout = []time.Duration{-1}
	}

	message, err := c.consumer.ReadMessage(timeout[0])
	if err != nil {
		return nil, err
	}
	c.offsetController.UpdateOffset(&message.TopicPartition)

	return message.Value, nil
}

func (c *consumer) Commit() error {
	_, err := c.consumer.CommitOffsets(c.offsetController.GetOffsets())
	return err
}

func (c *consumer) Close() error {
	return c.consumer.Close()
}
