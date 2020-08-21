package discover

import (
	"testing"

	zkdao "sorakaq/dao"
)

func TestRegister(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	DiscoverClientInit()
	defer DiscoverClient.Close()
	err := DiscoverClient.Register("test", "192:168:1:2", 30000, "")
	if err != nil {
		panic(err.Error())
	}
	t.Log("Register")
}

func TestDeRegister(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	DiscoverClientInit()
	defer DiscoverClient.Close()
	err := DiscoverClient.Register("test", "192:168:1:3", 30000, "")
	if err != nil {
		panic(err.Error())
	}
	t.Log("Register")
	err = DiscoverClient.DeRegister("test", "192:168:1:3", 30000)
	if err != nil {
		panic(err.Error())
	}
	t.Log("DeRegister")
}

func TestDiscoverServices(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	DiscoverClientInit()
	defer DiscoverClient.Close()
	err := DiscoverClient.Register("test", "192:168:1:4", 30000, "")
	if err != nil {
		panic(err.Error())
	}
	t.Log("Register")
	var res []string
	res, err = DiscoverClient.DiscoverServices("test")
	if err != nil {
		panic(err.Error())
	}
	t.Log(res)
}

func TestGetInstanceInfo(t *testing.T) {
	zkdao.ZkDao = zkdao.NewZkDao("localhost:2181", 5)
	defer zkdao.ZkDao.Close()
	DiscoverClientInit()
	defer DiscoverClient.Close()
	instanceInfo, err := DiscoverClient.GetInstanceInfo("admin", "192:168:1:5:30000")
	if err != nil {
		panic(err.Error())
	}
	t.Log(instanceInfo)
}
