package service

import (
	"sorakaq/message"

	"github.com/samuel/go-zookeeper/zk"
)

type IWorkerService interface {
	initTimeWheel()
	initRpcServer()
	msgCallback(msg *message.DelayMsg) error
	process(msg *message.DelayMsg) error
	httpCallBack(msg *message.DelayMsg) error
	checkWorkerMsg(msg *message.DelayMsg) bool
	checkCallbackResult(msg *message.DelayMsg, response *message.Response, err error) error
	watchCenterChange() error
	watchCenterChildChange(watch <-chan zk.Event) error
	getCenterAddressCache() []string
	setCenterAddressCache() ([]string, error)
	getAllCenterAddress() ([]string, error)
	closeRpcConn(workers []string)
	sendMsg(address string, req *message.Request) error
	getOneCenterAddress(excludeCenterAddress []string) (string, error)
	getOneActiveCenterAddress(centers []string) string
	Stop()
	delBackupMsg(workerId, msgId string)
	addTimeMsgCallback(msg *message.DelayMsg, addTime uint64)
	msgRetry()
}

var WorkerService IWorkerService
