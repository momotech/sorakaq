package dao

import (
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type kafkaBrokerDao struct {
	Broker *sarama.Broker
}

func NewKafkaBrokerDao(addr string) IKafkaBrokerDao {
	var kafkaBroker = &kafkaBrokerDao{}

	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_2

	client, err := sarama.NewClient(strings.Split(addr, ","), config)
	if err != nil {
		log.Errorf("sarama.NewClient addr : %s,  error : %s", addr, err)
		return nil
	}

	brokers := client.Brokers()
	rand.Seed(time.Now().Unix())
	broker := brokers[rand.Intn(len(brokers))]
	log.Debugf("client.Brokers : %v, rand Broker : %v", brokers, broker)

	err = broker.Open(config)
	if err != nil {
		log.Errorf("NewKafkaDao NewBroker error : %s, config : %v", err, config)
		return nil
	}

	connected, err := broker.Connected()
	if err != nil || !connected {
		log.Errorf("NewKafkaDao connected %t , err : %s", connected, err)
		return nil
	}
	kafkaBroker.Broker = broker
	return kafkaBroker
}

func (kd kafkaBrokerDao) Addr() string {
	return kd.Broker.Addr()
}

func (kd kafkaBrokerDao) CreateTopics(request *sarama.CreateTopicsRequest) (bool, error) {
	res, err := kd.Broker.CreateTopics(request)
	if err != nil {
		log.Errorf("CreateTopics error : %s, request : %v", err, request)
		return false, errors.New("CreateTopics error")
	}
	t := res.TopicErrors
	log.Debugf("res : %v, t : %v", res, t)
	return true, nil
}

func (kd kafkaBrokerDao) Close() {
	kd.Broker.Close()
}
