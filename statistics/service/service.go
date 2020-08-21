package service

import (
	"fmt"
	"strconv"
	"time"

	"sorakaq/config"
	"sorakaq/dao"

	log "github.com/sirupsen/logrus"
)

const (
	//topic维度消息流转平均耗时 hash 分钟级别统计
	topicMsgFlowAverageTime = "topic:msg:flow:average:time:%s:%s" //例如 msg:flow:time:delay_topic(主题名称):20200807
	//所有消息流转平均耗时 hash 分钟级别统计
	allMsgFlowAverageTime = "all:msg:flow:average:time:%s" //例如 msg:flow:time:20200807
	//topic维度消息回调平均耗时 hash 分钟级别统计
	topicMsgCallbackAverageTime = "topic:msg:callback:average:time:%s:%s" //例如 msg:callback:time:delay_topic(主题名称):20200807
	//所有消息回调平均耗时 hash 分钟级别统计
	allMsgCallbackAverageTime = "all:msg:callback:average:time:%s" //例如 msg:callback:time:20200807
	//topic维度消息个数 hash 分钟级别统计
	topicMsgCntStatistic = "topic:msg:cnt:%s:%s" // 例如 msg:cnt:delay_topic(主题名称):20200807
	//所有消息个数 hash 分钟级别统计
	allMsgCntStatistic = "all:msg:cnt:%s" // 例如 msg:cnt:20200807
	statisticScript    = "redis.call('hIncrBy', KEYS[1], tostring(ARGV[1]), tonumber(ARGV[2]));" +
		"redis.call('hIncrBy', KEYS[2], tostring(ARGV[1]), tonumber(ARGV[3]));" +
		"redis.call('hIncrBy', KEYS[3], tostring(ARGV[1]), tonumber(ARGV[4]));" +
		"redis.call('hIncrBy', KEYS[4], tostring(ARGV[1]), tonumber(ARGV[2]));" +
		"redis.call('hIncrBy', KEYS[5], tostring(ARGV[1]), tonumber(ARGV[3]));" +
		"redis.call('hIncrBy', KEYS[6], tostring(ARGV[1]), tonumber(ARGV[4]));return true"
)

type statisticsService struct {
}

func NewStatisticsService(conf config.RedisConfig) IStatisticsService {
	dao.InitRedisDao(conf)
	return &statisticsService{}
}

//记录消息耗时
func (this *statisticsService) RecordElapsedTime(topicName string, msgId string, flowElapsedTime uint64, callbackElapsedTime uint64) {
	log.WithFields(log.Fields{
		"topicName":                  topicName,
		"msgId":                      msgId,
		"currentFlowElapsedTime":     flowElapsedTime,
		"currentCallbackElapsedTime": callbackElapsedTime,
	}).Infof("Elapsed time statistical")
	date := time.Now().Format("20060102")
	t := time.Now()
	min := t.Hour()*60 + t.Minute()
	topicMsgFlowAverageTimeKey := fmt.Sprintf(topicMsgFlowAverageTime, topicName, date)
	allMsgFlowAverageTimeKey := fmt.Sprintf(allMsgFlowAverageTime, date)
	topicMsgCallbackAverageTimeKey := fmt.Sprintf(topicMsgCallbackAverageTime, topicName, date)
	allMsgCallbackAverageTimeKey := fmt.Sprintf(allMsgCallbackAverageTime, date)
	topicMsgCntStatisticKey := fmt.Sprintf(topicMsgCntStatistic, topicName, date)
	allMsgCntStatisticKey := fmt.Sprintf(allMsgCntStatistic, date)
	_, err := dao.RedisDao.SendScriptCommand(6, statisticScript, topicMsgFlowAverageTimeKey, topicMsgCallbackAverageTimeKey, topicMsgCntStatisticKey,
		allMsgFlowAverageTimeKey, allMsgCallbackAverageTimeKey, allMsgCntStatisticKey,
		min, flowElapsedTime, callbackElapsedTime, 1)
	if err != nil {
		log.Errorf("RecordElapsedTime err, %v", err)
	}
}

//查询消息耗时
func (this *statisticsService) QueryElapsedTime(topicName string, date string) (map[string][]interface{}, error) {

	elapsedTimeInfo := make(map[string]([]interface{}), 1440)
	elapsedTimeInfo["times"] = make([]interface{}, 1440)
	elapsedTimeInfo["flow_average_time"] = make([]interface{}, 1440)
	elapsedTimeInfo["callback_average_time"] = make([]interface{}, 1440)
	var msgFlowAverageTimeKey string
	var msgCallbackAverageTimeKey string
	var msgCntStatisticKey string
	if topicName == "" {
		msgFlowAverageTimeKey = fmt.Sprintf(allMsgFlowAverageTime, date)
		msgCallbackAverageTimeKey = fmt.Sprintf(allMsgCallbackAverageTime, date)
		msgCntStatisticKey = fmt.Sprintf(allMsgCntStatistic, date)
	} else {
		msgFlowAverageTimeKey = fmt.Sprintf(topicMsgFlowAverageTime, topicName, date)
		msgCallbackAverageTimeKey = fmt.Sprintf(topicMsgCallbackAverageTime, topicName, date)
		msgCntStatisticKey = fmt.Sprintf(topicMsgCntStatistic, topicName, date)
	}

	res1, err := dao.RedisDao.ExecRedisCommand("hgetall", msgFlowAverageTimeKey)
	if err != nil || res1 == nil {
		return elapsedTimeInfo, err
	}
	flowTimeInfo := res1.([]interface{})

	res2, err := dao.RedisDao.ExecRedisCommand("hgetall", msgCallbackAverageTimeKey)
	if err != nil || res2 == nil {
		return elapsedTimeInfo, err
	}
	callbackTimeInfo := res2.([]interface{})

	res3, err := dao.RedisDao.ExecRedisCommand("hgetall", msgCntStatisticKey)
	if err != nil || res3 == nil {
		return elapsedTimeInfo, err
	}
	msgStatisticInfo := res3.([]interface{})

	flowTimeMap := make(map[int]uint64)
	callbackTimeInfoMap := make(map[int]uint64)
	msgStatisticInfoMap := make(map[int]uint64)
	cnt := len(flowTimeInfo)
	for i := 0; i+1 < cnt; i = i + 2 {
		index, _ := strconv.Atoi(string(flowTimeInfo[i].([]byte)))
		flowTimeTotal, _ := strconv.ParseUint(string(flowTimeInfo[i+1].([]byte)), 10, 64)
		flowTimeMap[index] = flowTimeTotal
		callbackTimeTotal, _ := strconv.ParseUint(string(callbackTimeInfo[i+1].([]byte)), 10, 64)
		callbackTimeInfoMap[index] = callbackTimeTotal
		msgCnt, _ := strconv.ParseUint(string(msgStatisticInfo[i+1].([]byte)), 10, 64)
		msgStatisticInfoMap[index] = msgCnt
	}

	for i := 0; i < 1440; i++ {
		day, _ := time.ParseInLocation("20060102", date, time.Local)
		msTimestamp := day.Unix() + int64(i)*60
		_, ok := flowTimeMap[i]
		if !ok {
			elapsedTimeInfo["flow_average_time"][i] = 0
			elapsedTimeInfo["callback_average_time"][i] = 0
		} else {
			msgCnt := msgStatisticInfoMap[i]
			elapsedTimeInfo["flow_average_time"][i] = flowTimeMap[i] / msgCnt
			elapsedTimeInfo["callback_average_time"][i] = callbackTimeInfoMap[i] / msgCnt
		}
		elapsedTimeInfo["times"][i] = time.Unix(msTimestamp, 0).Format("2006-01-02 15:04:05")
	}
	return elapsedTimeInfo, nil
}
