package config

type AdminConfig struct {
	WebPort int64  `toml:"webport,omitempty"`
	RPCPort int64  `toml:"rpcport,omitempty"`
	Logs    string `toml:"logs,omitempty"`
	WebLogs string `toml:"weblogs,omitempty"`
}

type ReceiveConfig struct {
	WebPort int64  `toml:"webport,omitempty"`
	RPCPort int64  `toml:"rpcport,omitempty"`
	Logs    string `toml:"logs,omitempty"`
	WebLogs string `toml:"weblogs,omitempty"`
}

type CenterConfig struct {
	RPCPort int64  `toml:"rpcport,omitempty"`
	Logs    string `toml:"logs,omitempty"`
}

type WorkerConfig struct {
	RPCPort int64  `toml:"rpcport,omitempty"`
	Logs    string `toml:"logs,omitempty"`
}

type KafkaConfig struct {
	BrokerList  string `toml:"broker_list,omitempty"`
	TopicPrefix string `toml:"topic_prefix,omitempty"`
	User        string `toml:"user,omitempty"`
	Pass        string `toml:"pass,omitempty"`
}

type ZkConfig struct {
	Host           string `toml:"host,omitempty"`
	SessionTimeout int    `toml:"session_timeout,omitempty"`
}

type RedisConfig struct {
	Host           string `toml:"host,omitempty"`
	Db             int    `toml:"db,omitempty"`
	Password       string `toml:"password,omitempty"`
	MaxIdle        int    `toml:"maxIdle,omitempty"`        // 连接池最大空闲连接数
	MaxActive      int    `toml:"maxActive,omitempty"`      // 连接池最大激活连接数
	IdleTimeout    int    `toml:"idleTimeout,omitempty"`    // 连接池最大激活连接数
	ConnectTimeout int    `toml:"connectTimeout,omitempty"` // 连接超时, 单位毫秒
	ReadTimeout    int    `toml:"readTimeout,omitempty"`    // 读取超时, 单位毫秒
	WriteTimeout   int    `toml:"writeTimeout,omitempty"`   // 写入超时, 单位毫秒
}

type SorakaConf struct {
	KafkaConf   KafkaConfig   `toml:"kafka,omitempty"`
	ZkConf      ZkConfig      `toml:"zookeeper,omitempty"`
	RedisConf   RedisConfig   `toml:"redis,omitempty"`
	Logs        string        `toml:"logs,omitempty"`
	AdminConf   AdminConfig   `toml:"admin,omitempty"`
	ReceiveConf ReceiveConfig `toml:"receive,omitempty"`
	CenterConf  CenterConfig  `toml:"center,omitempty"`
	WorkerConf  WorkerConfig  `toml:"worker,omitempty"`
}
