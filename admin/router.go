package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
)

const (
	COOKIE_NAME = "sorakaq"
)

func startRouter(port int64) {
	router := gin.Default()
	router.LoadHTMLGlob("admin/templates/*")
	topic := router.Group("/sorakaq/topic")
	topic.Use(Auth())
	{
		topic.POST("/add", addTopic)
		topic.POST("/update", updateTopic)
		topic.GET("/list", TopicsManager)
		topic.POST("/get", getTopic)
	}
	iptopic := router.Group("/sorakaq/iptopic")
	iptopic.Use(Auth())
	{
		iptopic.POST("/get", getIpTopics)
	}
	instance := router.Group("/sorakaq/instance")
	instance.Use(Auth())
	{
		instance.GET("/list", Instances)
		instance.POST("/delete", deleteInstance)
	}
	msg := router.Group("/sorakaq/msg")
	msg.Use(Auth())
	{
		msg.POST("/list", getMsgList)
		msg.POST("/detail", getMsgDetail)
		msg.GET("/query", MsgQuery)
	}
	monitor := router.Group("/sorakaq/monitor")
	monitor.Use(Auth())
	{
		monitor.GET("/elapsedTime", getMsgElapsedTimeInfo)
		monitor.GET("/workerMsg", getWorkerMsg)
	}

	router.GET("/login", Login)
	router.GET("/monitor", Auth(), Monitor)
	router.GET("/monitorTest", Auth(), monitorTest)
	router.GET("/logoff", LogOff)
	router.POST("/sorakaq/login", LoginSystem)
	router.GET("/", Auth(), Index)
	go router.Run(fmt.Sprintf(":%d", port))
}
