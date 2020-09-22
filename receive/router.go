package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	receive "sorakaq/receive/service"

	"sorakaq/util"

	"sorakaq/message"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func startRouter(port int64) {
	router := gin.Default()
	msg := router.Group("/sorakaq/msg")
	{
		msg.POST("/submit", submit)
		msg.POST("/cancel", cancel)
		msg.POST("/addTime", addTime)
	}
	go router.Run(fmt.Sprintf(":%d", port))
}

func submit(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("Read body err, %v\n", err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()

	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil {
		log.Errorf("Unmarshal err: %v, %v", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	if req.TopicName == "" || req.Callback.RequestType == "" || req.Callback.Url == "" {
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}

	if req.DelayTime > util.SEVEN_DAY {
		c.JSON(http.StatusOK, util.RespJson(util.DELAY_TIME_EXCEEDS_THE_LIMIT, false))
		return
	}
	var msgId string
	msgId, err = receive.ReceiveService.Submit(req)
	if err != nil {
		log.Errorf("Submit req err, %v\n", err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	res := make(map[string]string)
	res["msgId"] = msgId
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, res))
}

func cancel(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("Read body err, %v\n", err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil {
		log.Errorf("Unmarshal err: %v, %v", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	if req.TopicName == "" || req.MsgId == "" {
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	res := receive.ReceiveService.Cancel(req)
	if err != nil {
		log.Errorf("Cancel req err, %v\n", err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, res))
}

func addTime(c *gin.Context) {
	b, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Errorf("Read body err, %v\n", err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	defer c.Request.Body.Close()
	req := &message.ReqParams{}
	err = json.Unmarshal(b, req)
	if err != nil {
		log.Errorf("Unmarshal err: %v, %v", err, string(b))
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	if req.TopicName == "" || req.MsgId == "" || req.AddTime == 0 || req.AddTime > util.SEVEN_DAY {
		c.JSON(http.StatusOK, util.RespJson(util.INVALID_PARAMS, false))
		return
	}
	res := receive.ReceiveService.AddTime(req)
	if err != nil {
		log.Errorf("AddTime req err, %v\n", err)
		c.JSON(http.StatusOK, util.RespJson(util.SYSTEM_ERROR, false))
		return
	}
	c.JSON(http.StatusOK, util.RespJson(util.SUCCESS, res))
}
