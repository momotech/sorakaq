package service

import (
	"fmt"

	"sorakaq/config"
	"sorakaq/dao"

	log "github.com/sirupsen/logrus"
)

type recoveryService struct{}

func NewRecoveryService(conf config.RedisConfig) IRecoveryService {
	dao.InitRedisDao(conf)
	return &recoveryService{}
}

const backupMsg = "backup:msg:%s"

const backupMsgDetail = "backup:msg:%s:detail"

const msgWorkerIdMap = "backup:msg:%s:workerId"

//增加worker消息
func (rs *recoveryService) AddWorkerMsg(workerId, msgId string, score int64, msgDetail string) bool {
	args := make([][]interface{}, 3)
	args[0] = []interface{}{"ZADD", fmt.Sprintf(backupMsg, workerId), score, msgId}
	args[1] = []interface{}{"HMSET", fmt.Sprintf(backupMsgDetail, workerId), msgId, msgDetail}
	args[2] = []interface{}{"SET", fmt.Sprintf(msgWorkerIdMap, msgId), workerId}
	err := dao.RedisDao.SendRedisPipeliningCommand(args)
	if err != nil {
		log.Errorf("AddWorkerMsg Error: %v", err)
		return false
	}
	return true
}

//根据消息id获取workerId
func (rs *recoveryService) GetWorkerIdByMsgId(msgId string) string {
	res, err := dao.RedisDao.ExecRedisCommand("GET", fmt.Sprintf(msgWorkerIdMap, msgId))
	if err != nil {
		log.Errorf("GetWorkerIdByMsgId Error, %v", err)
		return ""
	}
	if res == nil {
		return ""
	}
	return string(res.([]byte))
}

//删除worker备份消息
func (rs *recoveryService) DelWorkerMsg(workerId string, msgId string, isDelWorkerIdMap bool) bool {
	args := make([][]interface{}, 3)
	args[0] = []interface{}{"ZREM", fmt.Sprintf(backupMsg, workerId), msgId}
	args[1] = []interface{}{"HDEL", fmt.Sprintf(backupMsgDetail, workerId), msgId}
	if isDelWorkerIdMap {
		args[2] = []interface{}{"DEL", fmt.Sprintf(msgWorkerIdMap, msgId)}
	}
	err := dao.RedisDao.SendRedisPipeliningCommand(args)
	if err != nil {
		log.Errorf("DelWorkerMsg Error: %v", err)
		return false
	}
	return true
}

//根据分片获取worker备份消息
func (rs *recoveryService) GetWorkerMsgByShard(workerId string, start int, end int) (map[string]string, error) {
	res, err := dao.RedisDao.ExecRedisCommand("ZRANGE", fmt.Sprintf(backupMsg, workerId), start, end)
	if err != nil {
		return nil, err
	}
	var workerMsgMap = make(map[string]string)

	if res == nil || len(res.([]interface{})) == 0 {
		return workerMsgMap, nil
	}
	msgIds := res.([]interface{})

	var resDetail interface{}
	args := []interface{}{fmt.Sprintf(backupMsgDetail, workerId)}
	args = append(args, msgIds...)
	resDetail, err = dao.RedisDao.ExecRedisCommand("HMGET", args...)
	if err != nil || resDetail == nil {
		return workerMsgMap, err
	}
	msgResDetail := resDetail.([]interface{})
	for k, msgId := range msgIds {
		if msgResDetail[k] != nil {
			workerMsgMap[string(msgId.([]byte))] = string(msgResDetail[k].([]byte))
		} else {
			workerMsgMap[string(msgId.([]byte))] = ""
		}
	}
	return workerMsgMap, nil
}

//根据消息发送时间获取worker备份消息
func (rs *recoveryService) GetWorkerMsgByScore(workerId string, startScore uint64, limit int) ([]string, error) {
	res, err := dao.RedisDao.ExecRedisCommand("ZRANGEBYSCORE", fmt.Sprintf(backupMsg, workerId), startScore, "+inf", "limit", 0, limit)
	if err != nil {
		return nil, err
	}

	if res == nil || len(res.([]interface{})) == 0 {
		return []string{}, nil
	}
	msgIds := res.([]interface{})

	var resDetail interface{}
	args := []interface{}{fmt.Sprintf(backupMsgDetail, workerId)}
	args = append(args, msgIds...)
	resDetail, err = dao.RedisDao.ExecRedisCommand("HMGET", args...)
	if err != nil || resDetail == nil {
		return []string{}, err
	}
	var workerMsgs = make([]string, len(msgIds))
	msgResDetail := resDetail.([]interface{})
	for k, _ := range msgIds {
		if msgResDetail[k] != nil {
			workerMsgs[k] = string(msgResDetail[k].([]byte))
		} else {
			workerMsgs[k] = ""
		}
	}
	return workerMsgs, nil
}

//获取备份消息详情
func (rs *recoveryService) GetWorkerMsgByMsgId(workerId string, msgId string) (string, error) {
	res, err := dao.RedisDao.ExecRedisCommand("HGET", fmt.Sprintf(backupMsgDetail, workerId), msgId)
	if err != nil {
		log.Errorf("GetWorkerMsgByMsgId err workID:%s, msgID:%s, err:%v", workerId, msgId, err)
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return string(res.([]byte)), nil
}

//更新备份消息
func (rs *recoveryService) UpdateWorkerMsg(workerId, msgId string, msgDetail interface{}) bool {
	_, err := dao.RedisDao.ExecRedisCommand("HMSET", fmt.Sprintf(backupMsgDetail, workerId), msgId, msgDetail)
	if err != nil {
		log.Errorf("UpdateWorkerMsg HMset Error, %v", err)
		return false
	}
	return true
}

func (rs *recoveryService) GetWorkeMsgTotalNum(workerId string) (int64, error) {
	res, err := dao.RedisDao.ExecRedisCommand("HLEN", fmt.Sprintf(backupMsgDetail, workerId))
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (rs *recoveryService) GetWorkeMsgNumByTimeRange(workerId string, startTime int64, endTime int64) (int64, error) {
	res, err := dao.RedisDao.ExecRedisCommand("ZCOUNT", fmt.Sprintf(backupMsg, workerId), startTime, endTime)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}
