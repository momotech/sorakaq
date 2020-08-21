package dao

import "github.com/samuel/go-zookeeper/zk"

type IZkDao interface {
	WatchChildNode(path string) (<-chan zk.Event, error)
	GetChildrenNode(path string) ([]string, error)
	Get(path string) ([]byte, *zk.Stat, error)
	Set(path string, data []byte, version int32) (bool, error)
	Exists(path string) (bool, *zk.Stat, error)
	Lock(path string) error
	Unlock(path string) error
	EnsureNode(path string) error
	Create(path string, data []byte, flags int32) (string, error)
	Delete(path string, version int32) error
	Close()
}

type IRedisDao interface {
	ExecRedisCommand(command string, args ...interface{}) (interface{}, error)
	SendRedisPipeliningCommand(args [][]interface{}) error
	SendScriptCommand(keyCount int, src string, args ...interface{}) (interface{}, error)
}

var ZkDao IZkDao

var RedisDao IRedisDao
