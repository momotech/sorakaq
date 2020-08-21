package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	dao "sorakaq/dao"
	"sorakaq/discover"
	receive "sorakaq/receive/service"
	recovery "sorakaq/recovery/service"

	"sorakaq/util"

	"sorakaq/config"

	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

var confFile = flag.String("conf", "", "conf file path")

func main() {
	defer func() {
		if v := recover(); v != nil {
			log.Panicf("[main] recover panic: %s\n", v)
		}
	}()
	// 初始化配置
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
	if sorakaConf.ReceiveConf.Logs == "" {
		log.SetOutput(os.Stdout)
	} else {
		logFile := util.OpenFile(sorakaConf.ReceiveConf.Logs)
		defer logFile.Close()
		log.SetOutput(logFile)
	}
	log.SetLevel(log.DebugLevel)

	dao.ZkDaoInit(sorakaConf.ZkConf.Host, sorakaConf.ZkConf.SessionTimeout)
	defer dao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(sorakaConf.RedisConf)
	discover.DiscoverClientInit()
	ip := util.GetLocalIp()
	err = discover.DiscoverClient.Register(util.RECEIVE_SERVICE, ip, sorakaConf.ReceiveConf.RPCPort, "")
	if err != nil {
		panic(err.Error())
	}
	defer discover.DiscoverClient.DeRegister(util.RECEIVE_SERVICE, ip, sorakaConf.ReceiveConf.RPCPort)

	receive.ReceiveService, err = receive.NewReceiveService(sorakaConf.KafkaConf.BrokerList)
	if err != nil {
		panic(err.Error())
	}
	defer receive.ReceiveService.Stop()
	if sorakaConf.ReceiveConf.WebLogs != "" {
		f, _ := os.Create(sorakaConf.ReceiveConf.WebLogs)
		gin.DefaultWriter = io.MultiWriter(f)
	}
	startRouter(sorakaConf.ReceiveConf.WebPort)
	log.Infof("Receive service start")
	util.Wait()
}
