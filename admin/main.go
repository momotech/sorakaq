package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	admin "sorakaq/admin/service"

	"sorakaq/util"

	"sorakaq/discover"
	recovery "sorakaq/recovery/service"
	statistics "sorakaq/statistics/service"

	"sorakaq/config"
	zkdao "sorakaq/dao"

	"sorakaq/admin/dao"

	"github.com/gin-gonic/gin"

	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
)

var confFile = flag.String("conf", "", "conf file path")

func main() {
	defer func() {
		if v := recover(); v != nil {
			fmt.Println("panic")
			log.Panicf("[main] recover panic: %s\n", v)
		}
	}()

	flag.Parse()
	var sorakaConf config.SorakaConf
	_, err := toml.DecodeFile(*confFile, &sorakaConf)
	if err != nil {
		fmt.Errorf("Parse conf file error:%s", err)
		return
	}
	sorakaConfJSON, _ := json.Marshal(sorakaConf)
	log.Debugf("ConfFile : %s, sorakaConf : %s, ", *confFile, sorakaConfJSON)

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02 15:04:05.000000"})
	if sorakaConf.AdminConf.Logs == "" {
		log.SetOutput(os.Stdout)
	} else {
		logFile := util.OpenFile(sorakaConf.AdminConf.Logs)
		defer logFile.Close()
		log.SetOutput(logFile)
	}
	log.SetLevel(log.InfoLevel)

	zkdao.ZkDaoInit(sorakaConf.ZkConf.Host, sorakaConf.ZkConf.SessionTimeout)
	defer zkdao.ZkDao.Close()
	dao.KafkaDao = dao.NewKafkaBrokerDao(sorakaConf.KafkaConf.BrokerList)
	defer dao.KafkaDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(sorakaConf.RedisConf)
	admin.TopicService = admin.NewTopicService(sorakaConf.KafkaConf.TopicPrefix)
	discover.DiscoverClientInit()
	ip := util.GetLocalIp()
	err = discover.DiscoverClient.Register(util.ADMIN_SERVICE, ip, sorakaConf.AdminConf.RPCPort, "")
	if err != nil {
		panic(err.Error())
	}
	defer discover.DiscoverClient.DeRegister(util.ADMIN_SERVICE, ip, sorakaConf.AdminConf.RPCPort)
	statistics.StatisticsService = statistics.NewStatisticsService(sorakaConf.RedisConf)
	admin.Init()
	if sorakaConf.AdminConf.WebLogs != "" {
		f, _ := os.Create(sorakaConf.AdminConf.WebLogs)
		gin.DefaultWriter = io.MultiWriter(f)
	}
	startRouter(sorakaConf.AdminConf.WebPort)
	log.Infof("Admin service start")
	util.Wait()
}
