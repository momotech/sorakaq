package dao

import (
	"time"

	"sorakaq/config"

	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
)

type redisDao struct {
	redisPool *redis.Pool
}

var (
	redisConf config.RedisConfig
)

func InitRedisDao(conf config.RedisConfig) IRedisDao {
	redisConf = conf
	if RedisDao == nil {
		RedisDao = NewRedisDao()
	}
	return RedisDao
}

func NewRedisDao() IRedisDao {
	// 初始化连接池
	pool := &redis.Pool{
		MaxIdle:      redisConf.MaxIdle,
		MaxActive:    redisConf.MaxActive,
		IdleTimeout:  time.Duration(redisConf.IdleTimeout) * time.Second,
		Dial:         redisDial,
		TestOnBorrow: redisTestOnBorrow,
		Wait:         true,
	}
	return &redisDao{redisPool: pool}
}

// 连接redis
func redisDial() (redis.Conn, error) {
	conn, err := redis.Dial(
		"tcp",
		redisConf.Host,
		redis.DialConnectTimeout(time.Duration(redisConf.ConnectTimeout)*time.Millisecond),
		redis.DialReadTimeout(time.Duration(redisConf.ReadTimeout)*time.Millisecond),
		redis.DialWriteTimeout(time.Duration(redisConf.WriteTimeout)*time.Millisecond),
	)
	if err != nil {
		log.Printf("Connect redis err:%s", err.Error())
		return nil, err
	}

	if redisConf.Password != "" {
		if _, err := conn.Do("AUTH", redisConf.Password); err != nil {
			conn.Close()
			log.Errorf("Redis auth err: %s", err.Error())
			return nil, err
		}
	}

	_, err = conn.Do("SELECT", redisConf.Db)
	if err != nil {
		conn.Close()
		log.Errorf("Select db %d err: %s", redisConf.Db, err.Error())
		return nil, err
	}

	return conn, nil
}

// 从池中取出连接后，判断连接是否有效
func redisTestOnBorrow(conn redis.Conn, t time.Time) error {
	_, err := conn.Do("PING")
	if err != nil {
		log.Errorf("Get conn from pool err: %s", err.Error())
	}

	return err
}

func (r *redisDao) ExecRedisCommand(command string, args ...interface{}) (interface{}, error) {
	redis := r.redisPool.Get()
	defer redis.Close()
	return redis.Do(command, args...)
}

func (r *redisDao) SendRedisPipeliningCommand(args [][]interface{}) error {
	redis := r.redisPool.Get()
	defer redis.Close()
	for _, v := range args {
		if v == nil {
			break
		}
		log.Tracef("v:%v", v)
		redis.Send(v[0].(string), v[1:]...)
	}
	return redis.Flush()
}

func (r *redisDao) SendScriptCommand(keyCount int, src string, args ...interface{}) (interface{}, error) {
	conn := r.redisPool.Get()
	defer conn.Close()
	script := redis.NewScript(keyCount, src)
	return script.Do(conn, args...)
}
