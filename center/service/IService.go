package service

import (
	"sorakaq/message"

	"github.com/samuel/go-zookeeper/zk"
)

type ICenterService interface {
	Start()
	Stop()
	createConsumer(topics []string)
	createConsumerGroup(topics []string)
	getAllDelayTopics() ([]string, error)
	isDelayTopic(topic string) bool
	backupMsg(delayMsg *message.DelayMsg) bool
	sendMsg(address string, req *message.Request) error
	msgCallback(msg []byte, isDownWorkerMsg bool) error
	msgCallbackWorker(request *message.Request) error
	getOneWorkerAddress(topic string, excludeWorkerAddress []string) (string, error)
	getOneActiveWorkerAddress(topicWorkers []string, allWorkers []string, topic string) (string, error)
	getZKTopicWorkerAddresss(topic string) ([]string, error)
	watchTopicChange() error
	watchTopicChildChange(watch <-chan zk.Event) error
	watchWorkerChange() error
	watchWorkerChildChange(watch <-chan zk.Event) error
	reSendDownWorkerMsg(downWorkerAddress []string)
	setWorkerAddressCache() ([]string, error)
	getTopicWorkerAddressCache(topic string) []string
	getWorkerAddressCache() []string
	getAllWorkerAddress() ([]string, error)
	pullDownWorkerMsgCallback(workerId string)
	closeRpcConn(workers []string)
	initRpcServer()
	delBackupMsg(workerId string, msgId string, isDelWorkerIdMap bool)
	msgRetry()
}

var CenterService ICenterService
