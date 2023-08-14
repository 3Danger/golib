package kbroker

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	Broker interface {
		Producer(askLevel string, msgMaxBytes int, topicName string, clientId string) (Producer, error)
		Consumer(topic, groupId string, partition ...int32) (Consumer, error)
	}
	broker struct {
		cnf *Config
	}

	Config struct {
		HostServer string `json:"hostServer" required:"true"`
		Username   string `json:"username" required:"true"`
		Password   string `json:"password" required:"true"`

		SecurityProtocol string `json:"securityProtocol" default:"SASL_PLAINTEXT"`
		SaslMechanism    string `json:"saslMechanism" default:"PLAIN"`
	}
)

func New(cnf Config) Broker {
	return &broker{
		cnf: &cnf,
	}
}

func (b *broker) Producer(askLevel string, msgMaxBytes int, topicName string, clientId string) (Producer, error) {
	configMap := kafka.ConfigMap{
		"security.protocol": b.cnf.SecurityProtocol,
		"sasl.mechanism":    b.cnf.SaslMechanism,
		"sasl.username":     b.cnf.Username,
		"sasl.password":     b.cnf.Password,

		"message.timeout.ms": 1000 * 15,
		"retry.backoff.ms":   100,
		"retries":            5,

		"message.max.bytes": msgMaxBytes,

		"bootstrap.servers": b.cnf.HostServer,
		"acks":              askLevel,
		"client.id":         clientId,
	}
	newProducer, err := kafka.NewProducer(&configMap)
	if err != nil {
		return nil, err
	}
	return &producer{producer: newProducer, topicName: topicName}, nil
}

func (b *broker) Consumer(topic, groupId string, partition ...int32) (Consumer, error) {
	if newConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"security.protocol":        b.cnf.SecurityProtocol,
		"sasl.mechanism":           b.cnf.SaslMechanism,
		"sasl.username":            b.cnf.Username,
		"sasl.password":            b.cnf.Password,
		"bootstrap.servers":        b.cnf.HostServer,
		"group.id":                 groupId,
		"auto.offset.reset":        "beginning", //Если потребитель подкл впервые, то (earlest) читать с самого начала
		"enable.auto.offset.store": false,       //Потребитель не сохраняет смещение автоматически (false), мы управляем смещениями самостоятельно.
		"enable.auto.commit":       false,       //
	}); err != nil {
		return nil, err
	} else {
		if len(partition) != 0 {
			err = newConsumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: partition[0]}})
		} else {
			err = newConsumer.Subscribe(topic, nil)
		}
		if err != nil {
			return nil, err
		}

		if _, err = requireTopicPartition(newConsumer, topic, kafka.PartitionAny); err != nil {
			return nil, err
		}
		return &consumer{
				consumer:         newConsumer,
				offsetController: CreateOffsetController(),
			},
			nil

	}
}

func requireTopicPartition(consumer *kafka.Consumer, topic string, partition int32) (tp *kafka.TopicPartition, err error) {
	md, err := consumer.GetMetadata(&topic, false, 10000)
	if err != nil {
		//if ek, ok := err.(kafka.Error); !ok || ek.Code() != kafka.ErrTransport {
		return nil, fmt.Errorf("failed to get metadata for topic '%s': %w", topic, err)
		//}
	}
	if tmd, ok := md.Topics[topic]; ok {
		for _, p := range tmd.Partitions {
			if p.ID == partition {
				tp = &kafka.TopicPartition{
					Topic:     &topic,
					Partition: partition,
					Offset:    kafka.OffsetInvalid,
				}
				break
			}
		}
	}
	return tp, nil
}
