package service

import (
	"testing"

	"sorakaq/config"
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

func TestAddWorkerMsg(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res := RecoveryService.AddWorkerMsg("3232235784", "abc", 100, "test")
	t.Log("add worker msg res:", res)
}

func TestGetWorkerIdByMsgId(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res := RecoveryService.GetWorkerIdByMsgId("abc")
	t.Log("get worker id, res:", res)
}

func TestGetWorkerMsgByShard(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res, err := RecoveryService.GetWorkerMsgByShard("3232235784", 0, 10)
	if err != nil {
		panic(err.Error())
	}
	t.Log("get worker msg by shard, res:", res)
}

func TestGetWorkerMsgByMsgId(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res, err := RecoveryService.GetWorkerMsgByMsgId("3232235784", "abc")
	t.Log("get worker msg by msg id, res:", res, " err: ", err)
}

func TestUpdateWorkerMsg(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res := RecoveryService.UpdateWorkerMsg("3232235784", "abc", "testnew")
	t.Log("update worker msg, res:", res)
}

func TestDelWorkerMsg(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res := RecoveryService.DelWorkerMsg("3232235784", "abc", true)
	t.Log("del worker msg, res:", res)
	res = RecoveryService.DelWorkerMsg("3232235784", "abc", false)
	t.Log("del worker msg, res:", res)
}

func TestGetWorkerMsgByScore(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res, err := RecoveryService.GetWorkerMsgByScore("2886756163", 1596083145407, 10)
	if err != nil {
		panic(err.Error())
	}
	t.Log("get worker msg by score, res:", res)
}

func TestGetWorkeMsgTotalNum(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res, err := RecoveryService.GetWorkeMsgTotalNum("2886756163")
	if err != nil {
		panic(err.Error())
	}
	t.Log("get worker msg total num, res:", res)
}

func TestGetWorkeNumByTimeRange(t *testing.T) {
	RecoveryService = NewRecoveryService(testRedisConf)
	res, err := RecoveryService.GetWorkeMsgNumByTimeRange("2886756163", 1596083155188, 1596083155255)
	if err != nil {
		panic(err.Error())
	}
	t.Log("get worker msg num by time range, res:", res)
}
