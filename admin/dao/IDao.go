package dao

import (
	"github.com/Shopify/sarama"
)

type IKafkaBrokerDao interface {
	Addr() string
	CreateTopics(request *sarama.CreateTopicsRequest) (bool, error)
	Close()
}

var KafkaDao IKafkaBrokerDao
