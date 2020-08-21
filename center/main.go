package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"sorakaq/util"

	center "sorakaq/center/service"
	"sorakaq/config"
	dao "sorakaq/dao"
	"sorakaq/discover"
	recovery "sorakaq/recovery/service"

	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
)

var confFile = flag.String("conf", "", "conf file path")

func main() {
	defer func() {
		if v := recover(); v != nil {
			log.Fatalf("[main] recover panic: %s\n", v)
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
	if sorakaConf.CenterConf.Logs == "" {
		log.SetOutput(os.Stdout)
	} else {
		logFile := util.OpenFile(sorakaConf.CenterConf.Logs)
		defer logFile.Close()
		log.SetOutput(logFile)
	}
	log.SetLevel(log.InfoLevel)

	dao.ZkDaoInit(sorakaConf.ZkConf.Host, sorakaConf.ZkConf.SessionTimeout)
	defer dao.ZkDao.Close()

	recovery.RecoveryService = recovery.NewRecoveryService(sorakaConf.RedisConf)
	discover.DiscoverClientInit()
	ip := util.GetLocalIp()
	err = discover.DiscoverClient.Register(util.CENTER_SERVICE, ip, sorakaConf.CenterConf.RPCPort, "")
	if err != nil {
		panic(err.Error())
	}
	defer discover.DiscoverClient.DeRegister(util.CENTER_SERVICE, ip, sorakaConf.CenterConf.RPCPort)
	center.Init(sorakaConf.KafkaConf.BrokerList, sorakaConf.KafkaConf.TopicPrefix, ip, sorakaConf.CenterConf.RPCPort)
	center.CenterService.Start()
	log.Infof("Center service start")
	defer center.CenterService.Stop()
	util.Wait()
}
