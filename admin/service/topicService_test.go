package service

import (
	"fmt"
	"testing"

	"sorakaq/admin/dao"
	"sorakaq/admin/entity"
	"sorakaq/config"
	zkdao "sorakaq/dao"
	"sorakaq/discover"
	recovery "sorakaq/recovery/service"
	"sorakaq/util"
)

var testRedisConf config.RedisConfig = config.RedisConfig{
	Host:           "127.0.0.1:6379",
	Db:             0,
	Password:       "",
	MaxIdle:        100,
	MaxActive:      1000,
	IdleTimeout:    300,
	ConnectTimeout: 3000,
	ReadTimeout:    3000,
	WriteTimeout:   3000,
}

func TestCreate(t *testing.T) {
	dao.KafkaDao = dao.NewKafkaBrokerDao("localhost:9092")
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	defer dao.KafkaDao.Close()
	topicService := NewTopicService("")

	topic := &entity.Topic{}
	topic.TopicName = "delay-test1"
	topic.Business = "test1"
	topic.WorkerNum = 1

	ret, err := topicService.Create(topic)

	fmt.Println("ret : ", ret, ", err : ", err)
}

func TestUpdate(t *testing.T) {
	dao.KafkaDao = dao.NewKafkaBrokerDao("localhost:9092")
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	defer dao.KafkaDao.Close()
	topicService := NewTopicService("")

	topic := &entity.Topic{}
	topic.TopicName = "delay-test1"
	topic.Business = "test1"
	topic.WorkerNum = 2

	ret, err := topicService.Create(topic)

	fmt.Println("Ret: ", ret, ", err : ", err)
}

func TestGetList(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(testRedisConf)
	topicService := NewTopicService("")
	topics, err := topicService.GetList()
	fmt.Println("Topics: ", topics, ", err: ", err)
}

func TestGet(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(testRedisConf)
	topicService := NewTopicService("")
	topicInfo, err := topicService.Get("delay_test")
	fmt.Println("TopicInfo: ", topicInfo, ", err: ", err)
}

func TestGetIpTopics(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(testRedisConf)
	topicService := NewTopicService("")
	topics, err := topicService.GetIpTopics("192.168.4.4:30002")
	fmt.Println("Topics : ", topics, ", err: ", err)
}

func TestGetInstanceList(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(testRedisConf)
	discover.DiscoverClientInit()
	defer discover.DiscoverClient.Close()
	topicService := NewTopicService("")
	instances, err := topicService.GetInstanceList("worker")
	fmt.Println("Instances : ", instances, ", err : ", err)
}

func TestDeleteInstance(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(testRedisConf)
	discover.DiscoverClientInit()
	defer discover.DiscoverClient.Close()
	topicService := NewTopicService("")
	err := topicService.DeleteInstance(util.WORKER_SERVICE, "192.168.4.4", 30002)
	fmt.Println("Err : ", err)
}
