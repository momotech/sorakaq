package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"sorakaq/admin/entity"
	"sorakaq/admin/service"
	"sorakaq/discover"
	"sorakaq/message"
	recovery "sorakaq/recovery/service"
	statistics "sorakaq/statistics/service"
	"sorakaq/util"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

var invalidTopics = map[string]bool{"admin": true, "center": true, "worker": true, "receive": true, "lock": true, "iptopic": true}

var validServiceName = map[string]bool{"admin": true, "center": true, "worker": true, "receive": true, "all": true}

func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		cookie, e := c.Request.Cookie(COOKIE_NAME)
		if e == nil {
			log.Info(c.Request.URL)
			c.SetCookie(cookie.Name, cookie.Value, 3600*3, cookie.Path, cookie.Domain, cookie.Secure, cookie.HttpOnly)
			c.Next()
		} else {
			log.Infof("cookie %s is not exist", COOKIE_NAME)
			c.Abort()
			c.Redirect(http.StatusMovedPermanently, "/login")
		}
	}
}

func Instances(c *gin.Context) {

	serviceNames := []string{util.ADMIN_SERVICE, util.CENTER_SERVICE, util.WORKER_SERVICE, util.RECEIVE_SERVICE}
	data := make([]entity.Instance, 0)
	for _, serviceName := range serviceNames {
		instances, err := service.TopicService.GetInstanceList(serviceName)
		if err != nil || len(instances) == 0 {
			continue
		}
		for _, instance := range instances {
			ins := &entity.Instance{
				ServiceName: serviceName,
				IPPort:      instance,
				Weight:      1,
			}
			instanceInfo, err := discover.DiscoverClient.GetInstanceInfo(serviceName, instance)
			if err != nil {
				log.Error("Get instance info err, serviceName : %s, instance : %s", serviceName, instance)

			} else {
				ins.CreateTime = instanceInfo.CreateTime
			}
			data = append(data, *ins)
		}
	}
	c.HTML(http.StatusOK, "instance.tmpl", gin.H{
		"title": "实例管理",
		"data":  data,
	})
}

func TopicsManager(c *gin.Context) {

	data := make([]entity.TopicZkInfo, 0)
	topics, err := service.TopicService.GetList()
	if err != nil {
		c.HTML(http.StatusOK, "topics.tmpl", gin.H{
			"title": "主题管理",
			"data":  data,
		})
		return
	}
	for _, topic := range topics {
		TopicZkInfo, err := service.TopicService.Get(topic)
		if err != nil {
			continue
		}
		data = append(data, TopicZkInfo)
	}
	c.HTML(http.StatusOK, "topics.tmpl", gin.H{
		"title": "主题管理",
		"data":  data,
	})
}

func MsgQuery(c *gin.Context) {
	c.HTML(http.StatusOK, "query.tmpl", gin.H{})
}

func Index(c *gin.Context) {
	c.Redirect(http.StatusMovedPermanently, "/sorakaq/topic/list")
}

func Login(c *gin.Context) {
	c.HTML(http.StatusOK, "login.tmpl", gin.H{})
}

func Monitor(c *gin.Context) {
	c.HTML(http.StatusOK, "monitor.tmpl", gin.H{})
}

func monitorTest(c *gin.Context) {
	data := make(map[string]interface{})
	data["worker1"] = []int{10, 50, 80, 100}
	data["worker2"] = []int{30, 70, 120, 200}
	data["worker3"] = []int{25, 60, 70, 140}
	data["worker4"] = []int{35, 55, 90, 115}
	data["workers"] = []string{"worker1", "worker2", "worker3", "worker4"}
	data["names"] = []string{"2020-10-10", "2020-10-11", "2020-10-12", "2020-10-14"}

	c.JSON(http.StatusOK, data)
}

func LogOff(c *gin.Context) {
	cookie, e := c.Request.Cookie(COOKIE_NAME)
	if e == nil {
		c.SetCookie(COOKIE_NAME, "", -1, cookie.Path, cookie.Domain, cookie.Secure, cookie.HttpOnly)
	}
	c.Redirect(http.StatusMovedPermanently, "/login")
}

func LoginSystem(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("LoginSystem Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil || req.Username == "" || req.Password == "" {
		log.Errorf("LoginSystem req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INCORRECT_ACCOUNT_OR_PASSWORD, false))
		return
	}
	if req.Username != "admin" || req.Password != "admin" {
		c.JSON(http.StatusOK, util.RespJson(util.INCORRECT_ACCOUNT_OR_PASSWORD, false))
	}
	cookie := util.Md5V(req.Username + req.Password)
	c.SetCookie(COOKIE_NAME, cookie, 3600*3, "/", strings.Split(c.Request.Host, ":")[0], false, true)
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, true))
}

func addTopic(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("AddTopic Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()

	req := &entity.Topic{}
	err = json.Unmarshal(b, req)
	if err != nil || req.Business == "" || req.TopicName == "" || req.WorkerNum == 0 {
		log.Errorf("AddTopic req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	_, ok := invalidTopics[req.TopicName]
	if ok {
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_TOPIC_NAME, false))
		return
	}

	_, err = service.TopicService.Create(req)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, true))
}

func updateTopic(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("UpdateTopic Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()

	req := &entity.Topic{}
	err = json.Unmarshal(b, req)
	if err != nil || req.TopicName == "" || req.WorkerNum == 0 {
		log.Errorf("UpdateTopic req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	err = service.TopicService.Update(req)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, true))
}

func getTopicList(c *gin.Context) {
	topics, err := service.TopicService.GetList()
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	res := make(map[string]interface{})
	res["topics"] = topics
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, res))
}

func getTopic(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("GetTopic Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &entity.Topic{}
	err = json.Unmarshal(b, req)
	if err != nil || req.TopicName == "" {
		log.Errorf("GetTopic req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	TopicZkInfo, err := service.TopicService.Get(req.TopicName)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, TopicZkInfo))
}

func getIpTopics(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("GetIpTopics Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil || req.Instance == "" {
		log.Errorf("GetIpTopics req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	topics, err := service.TopicService.GetIpTopics(req.Instance)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	res := make(map[string]interface{})
	res["topics"] = topics
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, res))
}

func deleteInstance(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("DeleteWorkerInstance Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil || req.Instance == "" || req.ServiceName == "" {
		log.Errorf("DeleteWorkerInstance req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	instanceInfo := strings.Split(req.Instance, ":")
	if len(instanceInfo) != 2 {
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	port, err := strconv.ParseInt(instanceInfo[1], 10, 64)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	err = service.TopicService.DeleteInstance(req.ServiceName, instanceInfo[0], port)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, true))
}

func getMsgList(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("GetMsgList Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil || req.Instance == "" || req.MsgNum <= 0 {
		log.Errorf("GetMsgList req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	if req.MsgNum > 500 {
		c.JSON(http.StatusOK, util.RespJson(util.MSG_NUM_LIMIT, false))
		return
	}
	workerId := util.GetWorkerIdByWorkerIp(strings.Split(req.Instance, ":")[0])
	workerMsgs, err := recovery.RecoveryService.GetWorkerMsgByScore(workerId, req.StartTime, req.MsgNum+1)
	if err != nil {
		log.Errorf("Get worker msg failed, workerId : %s, err : %v", workerId, err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	total, err := recovery.RecoveryService.GetWorkeMsgTotalNum(workerId)
	if err != nil {
		log.Errorf("Get worker msg total num failed, workerId : %s, err : %v", workerId, err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	res := make(map[string]interface{})
	res["total"] = total
	res["remain"] = 0
	if len(workerMsgs) > req.MsgNum {
		res["remain"] = 1
	}
	lists := getConvertMsgList(workerMsgs)
	res["list"] = lists[:len(lists)-1]
	lastMsg := lists[len(lists)-1]
	res["end_time"] = lastMsg.SendTime + lastMsg.DelayTime
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, res))
}

func getConvertMsgList(workerMsgs []string) []message.DelayMsg {
	lists := make([]message.DelayMsg, 0)
	for _, msg := range workerMsgs {
		if msg == "" {
			continue
		}
		delayMsg := new(message.DelayMsg)
		err := json.Unmarshal([]byte(msg), &delayMsg)
		if err != nil {
			log.Errorf("Unmarshal msg err, msg:%s, err:%v", msg, err)
		}
		lists = append(lists, *delayMsg)
	}
	return lists
}

func getMsgDetail(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("GetMsgDetail Read body err, %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil || req.MsgId == "" {
		log.Errorf("GetMsgDetail req: %v, %s", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	workerId := recovery.RecoveryService.GetWorkerIdByMsgId(req.MsgId)
	if len(workerId) == 0 {
		c.JSON(http.StatusOK, util.RespJson(util.MSG_NOT_EXIST, false))
		return
	}
	delayMsgStr, err := recovery.RecoveryService.GetWorkerMsgByMsgId(workerId, req.MsgId)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.MSG_NOT_EXIST, false))
		return
	}
	delayMsg := new(message.DelayMsg)
	err = json.Unmarshal([]byte(delayMsgStr), &delayMsg)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	delayMsg.WorkerIp = util.GetWorkerIpByWorkerId(delayMsg.WorkerId)
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, *delayMsg))
}

func getMsgElapsedTimeInfo(c *gin.Context) {
	date := time.Now().Format("20060102")
	res, err := statistics.StatisticsService.QueryElapsedTime("", date)
	if err != nil {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, res)
}

func getWorkerMsg(c *gin.Context) {
	instances, err := service.TopicService.GetInstanceList(util.WORKER_SERVICE)
	if err != nil || len(instances) == 0 {
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	timeSlice := make([]interface{}, 7)
	workers := make([]interface{}, len(instances))
	timeStr := time.Now().Format("2006-01-02")
	day, _ := time.ParseInLocation("2006-01-02", timeStr, time.Local)
	workerMsgMap := make(map[string]([]interface{}))
	for i := 0; i < 7; i++ {
		timeSlice[i] = day.Format("2006-01-02")
		msTimestamp := day.Unix() * 1000
		nextDayMsTimestamp := day.AddDate(0, 0, 1).Unix() * 1000
		for k, instance := range instances {
			workerIp := strings.Split(instance, ":")[0]
			workerId := util.GetWorkerIdByWorkerIp(workerIp)
			workers[k] = workerIp
			num, _ := recovery.RecoveryService.GetWorkeMsgNumByTimeRange(workerId, msTimestamp, nextDayMsTimestamp)
			workerMsgMap[workerIp] = append(workerMsgMap[workerIp], num)
		}
		day = time.Unix(nextDayMsTimestamp/1000, 0)
	}
	workerMsgMap["names"] = timeSlice
	workerMsgMap["workers"] = workers
	c.JSON(http.StatusOK, workerMsgMap)
}
