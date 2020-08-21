package service

type IRecoveryService interface {
	AddWorkerMsg(workerId string, msgId string, score int64, msgDetail string) bool
	DelWorkerMsg(workerId string, msgId string, isDelWorkerIdMap bool) bool
	GetWorkerMsgByShard(workerId string, start int, end int) (map[string]string, error)
	GetWorkerIdByMsgId(msgId string) string
	GetWorkerMsgByMsgId(workerId string, msgId string) (string, error)
	UpdateWorkerMsg(workerId, msgId string, msgDetail interface{}) bool
	GetWorkerMsgByScore(workerId string, startScore uint64, limit int) ([]string, error)
	GetWorkeMsgTotalNum(workerId string) (int64, error)
	GetWorkeMsgNumByTimeRange(workerId string, startTime int64, endTime int64) (int64, error)
}

var RecoveryService IRecoveryService
