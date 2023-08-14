package kbroker

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
)

type (
	OffsetController interface {
		UpdateOffset(newOffset *kafka.TopicPartition)
		GetOffsets() kafka.TopicPartitions
	}
	offsetController struct {
		data map[keyType]*kafka.TopicPartition
		rw   *sync.RWMutex
	}
	keyType struct {
		topic     string
		partition int32
	}
)

func CreateOffsetController() OffsetController {
	return &offsetController{
		data: map[keyType]*kafka.TopicPartition{},
		rw:   new(sync.RWMutex),
	}
}

func (o *offsetController) UpdateOffset(newOffset *kafka.TopicPartition) {
	key := keyType{
		topic:     *newOffset.Topic,
		partition: newOffset.Partition,
	}
	o.rw.Lock()
	if oldOffset, ok := o.data[key]; !ok || oldOffset.Offset <= newOffset.Offset {
		newOffset.Offset++
		o.data[key] = newOffset
	}
	o.rw.Unlock()
}

func (o *offsetController) GetOffsets() kafka.TopicPartitions {
	var result []kafka.TopicPartition
	o.rw.RLock()
	result = make([]kafka.TopicPartition, 0, len(o.data))
	for k := range o.data {
		result = append(result, *o.data[k])
	}
	o.rw.RUnlock()
	return result
}
