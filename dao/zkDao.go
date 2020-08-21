package dao

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"sorakaq/util"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type zkDao struct {
	zkCli *zk.Conn
	m     *sync.Mutex
}

func (zt *zkDao) EnsureNode(path string) error {
	zt.m.Lock()
	defer zt.m.Unlock()
	exists, _, err := zt.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err := zt.Create(path, []byte(""), 0)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func (zt *zkDao) Create(path string, data []byte, flags int32) (string, error) {
	res, err := zt.zkCli.Create(path, data, flags, zk.WorldACL(zk.PermAll))
	if err != nil {
		return res, err
	}
	return res, err
}

func (zt *zkDao) Lock(childNodeName string) error {

	err := zt.EnsureNode(util.LockNode())
	if err != nil {
		log.Errorf("Create or get lock root node err, %v", err)
		return err
	}
	_, err = zt.Create(util.LockChildNode(childNodeName), []byte(""), zk.FlagEphemeral)
	if err != nil {
		return err
	}
	return nil
}

func (zt *zkDao) Unlock(childNodeName string) error {
	zt.zkCli.Delete(util.LockChildNode(childNodeName), 0)
	return nil
}

func ZkDaoInit(host string, sessionTimeOut int) IZkDao {
	if ZkDao == nil {
		ZkDao = NewZkDao(host, sessionTimeOut)
	}
	return ZkDao
}

func NewZkDao(host string, sessionTimeOut int) IZkDao {
	var zkClient = &zkDao{m: new(sync.Mutex)}
	var err error
	zkClient.zkCli, _, err = zk.Connect(strings.Split(host, ","), time.Duration(sessionTimeOut)*time.Second)
	if err != nil {
		panic("Error get zkClient")
	}
	return zkClient
}

func (zt *zkDao) WatchChildNode(path string) (<-chan zk.Event, error) {
	_, _, ech, err := zt.zkCli.ChildrenW(path)
	if err != nil {
		return nil, err
	}
	return ech, nil
}

func (zt *zkDao) GetChildrenNode(path string) ([]string, error) {
	childrens, _, err := zt.zkCli.Children(path)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Get children node err, path: %s, err, %v", path, err))
	}
	return childrens, nil
}

func (zt *zkDao) Get(path string) ([]byte, *zk.Stat, error) {
	return zt.zkCli.Get(path)
}

func (zt *zkDao) Set(path string, data []byte, version int32) (bool, error) {
	stat, err := zt.zkCli.Set(path, data, version)
	if err != nil {
		return false, err
	}
	if stat.Version <= version {
		return false, errors.New("Zk Set data fail")
	}
	return true, nil
}

func (zt *zkDao) Exists(path string) (bool, *zk.Stat, error) {
	return zt.zkCli.Exists(path)
}

func (zt *zkDao) Delete(path string, version int32) error {
	return zt.zkCli.Delete(path, version)
}

func (zt *zkDao) Close() {
	if zt.zkCli != nil {
		zt.zkCli.Close()
	}
}
