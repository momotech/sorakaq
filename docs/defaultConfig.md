### config/config.toml
```
#可以自定义修改配置
[admin]
webport = 9087 #http请求端口
rpcport = 30000 #服务注册端口
logs = "/tmp/admin.log"#日志目录

[center]
rpcport = 30001 #服务注册端口
logs = "/tmp/center.log"#日志目录

[worker]
rpcport = 30002 #服务注册端口
logs = "/tmp/worker.log" #日志目录

[receive]
webport = 9088 #http请求端口
rpcport = 30003 #服务注册端口
logs = "/tmp/receive.log" #日志目录

[kafka]
broker_list = "localhost:9092"#kafka连接地址
topic_prefix = "" #延迟topic前缀，可以只监听特定前缀的topic，如果为""，监听所有topic
user = "xxx" #kafka连接账号 目前未使用
pass = "xxx" #kafka连接密码 目前未使用

[zookeeper]
host = "localhost:2181"#zookeeper连接地址
session_timeout = 5 #会话超时时间，单位秒

#建议redis开启持久化
[redis]
host = "127.0.0.1:6379" #redis连接地址
db = 0 #使用的redis数据库
password = "" #redis密码, 无需密码留空
maxIdle = 100 #redis连接池最大空闲连接数
maxActive = 1000 #redis连接池最大激活连接数, 0为不限制
idleTimeout = 300 #redis连接空闲超时时间, 单位秒
connectTimeout = 3000 #redis连接超时时间, 单位毫秒
readTimeout = 3000 #redis读取超时时间, 单位毫秒
writeTimeout = 3000 #redis写入超时时间, 单位毫秒
```