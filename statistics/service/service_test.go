package service

import (
	"fmt"
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

func TestRecordElapsedTime(t *testing.T) {
	StatisticsService = NewStatisticsService(testRedisConf)
	StatisticsService.RecordElapsedTime("delay_test", "111", 70, 100)
	StatisticsService.RecordElapsedTime("delay_test", "222", 30, 200)
	t.Log("record success")
}

func TestQueryElapsedTime(t *testing.T) {
	StatisticsService = NewStatisticsService(testRedisConf)
	res1, err := StatisticsService.QueryElapsedTime("", "20200807")
	fmt.Printf("query res: %v, err:%v\n", res1, err)
	res2, err := StatisticsService.QueryElapsedTime("delay_test", "20200807")
	fmt.Printf("query res: %v, err:%v\n", res2, err)
	t.Log("query success")
}
