package main

import (
	"encoding/json"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"

	dao "sorakaq/dao"
	"sorakaq/discover"

	worker "sorakaq/worker/service"

	recovery "sorakaq/recovery/service"

	statistics "sorakaq/statistics/service"

	"sorakaq/util"

	"sorakaq/config"

	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
)

var confFile = flag.String("conf", "", "conf file path")

//处理延时消息
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
	if sorakaConf.WorkerConf.Logs == "" {
		log.SetOutput(os.Stdout)
	} else {
		logFile := util.OpenFile(sorakaConf.WorkerConf.Logs)
		defer logFile.Close()
		log.SetOutput(logFile)
	}
	log.SetLevel(log.InfoLevel)

	dao.ZkDaoInit(sorakaConf.ZkConf.Host, sorakaConf.ZkConf.SessionTimeout)
	defer dao.ZkDao.Close()
	recovery.RecoveryService = recovery.NewRecoveryService(sorakaConf.RedisConf)
	discover.DiscoverClientInit()
	ip := util.GetLocalIp()
	err = discover.DiscoverClient.Register(util.WORKER_SERVICE, ip, sorakaConf.WorkerConf.RPCPort, "")
	if err != nil {
		panic("worker register err")
	}
	defer discover.DiscoverClient.DeRegister(util.WORKER_SERVICE, ip, sorakaConf.WorkerConf.RPCPort)
	statistics.StatisticsService = statistics.NewStatisticsService(sorakaConf.RedisConf)
	worker.Init(ip, sorakaConf.WorkerConf.RPCPort)
	log.Infof("Worker service start")
	defer worker.WorkerService.Stop()
	util.Wait()
}
