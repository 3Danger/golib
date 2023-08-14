package kbroker

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type (
	Producer interface {
		Produce(data []byte) error
		ProduceByPartition(data []byte, partition int32) error
		Close()
	}
	producer struct {
		producer  *kafka.Producer
		topicName string
	}
)

func (p *producer) Produce(data []byte) error {

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	err := p.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &p.topicName,
				Partition: kafka.PartitionAny,
			},
			Key:           nil,
			Timestamp:     time.Now(),
			TimestampType: kafka.TimestampCreateTime | kafka.TimestampLogAppendTime | kafka.AlterOperationSet,
			Value:         data,
		}, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	switch event := e.(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			return fmt.Errorf("while writing msg: %w", event.TopicPartition.Error)
		}
	case kafka.Error:
		return fmt.Errorf("kafka: %w", event)
	default:
		return fmt.Errorf("unknown event: %v", event)
	}
	return nil
}

func (p *producer) ProduceByPartition(data []byte, partition int32) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	err := p.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &p.topicName,
				Partition: partition,
			},
			Key:           nil,
			Timestamp:     time.Now(),
			TimestampType: kafka.TimestampCreateTime | kafka.TimestampLogAppendTime | kafka.AlterOperationSet,
			Value:         data,
		}, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	switch event := e.(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			return fmt.Errorf("while writing msg: %w", event.TopicPartition.Error)
		}
	case kafka.Error:
		return fmt.Errorf("kafka: %w", event)
	default:
		return fmt.Errorf("unknown event: %v", event)
	}
	return nil
}

func (p *producer) Close() {
	p.producer.Close()
}
