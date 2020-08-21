package service

import (
	"sorakaq/admin/entity"

	"github.com/samuel/go-zookeeper/zk"
)

type ITopicService interface {
	Create(*entity.Topic) (string, error)
	Update(*entity.Topic) error
	GetList() ([]string, error)
	Get(topic string) (entity.TopicZkInfo, error)
	GetIpTopics(ipPortStr string) ([]string, error)
	GetInstanceList(serviceName string) ([]string, error)
	DeleteInstance(serviceName, ip string, port int64) error
	updateIPTopic(handle ZkHandleType, IPS []string, topic string) (bool, error)
	watchWorkerChange() error
	watchWorkerChildChange(watch <-chan zk.Event) error
	setWorkerAddressCache() ([]string, error)
	getAllWorkerAddress() ([]string, error)
	getWorkerAddressCache() []string
	getAssignedWorkers(workerNum int, excludeWorkerAddress []string) ([]string, error)
	removeTopicIP(ipPortStr string) error
	updateTopicWorkerMap(topic, ipPortStr string, addWorkerNum int) error
	isDelayTopic(topic string) bool
}

var TopicService ITopicService
