### 整体架构
![avatar](images/Architecture.png)


### 模块介绍
1. receive模块主要功能：
- 消息接收（接收业务方提交的延迟消息，并将消息写入kafka；接收业务方提交的加时/撤回消息，并将消息写入redis） 
2. admin模块主要功能：
- topic管理（新增topic、增加和减少topic和worker映射，目前topic和worker列表映射是随机分配）
- 实例管理（查询receive/admin/center/worker实例、删除worker实例）
- 消息管理 (查询消息列表和消息详情)
- 性能指标查询（topic维度分钟级消息流转平均耗时和回调平均耗时查询）
3. center模块主要功能:
- 消息消费（通过kafka消息者组模式订阅所有延时topic，消息kafka中延时topic对应partition里的消息）
- 消息备份（备份延时消息到redis）
- 消息分发（根据负载均衡策略，将延时消息通过rpc协议分发给worker）
4. worker模块主要功能 
- 消息回调（接收center分发的消息，延时任务消息到达进行消息回调）

### 消息分发负载均衡策略
1. topic与worker资源动态分配策略
- 目前topic分配worker实例列表的策略是随机分配。
2. center消息分发策略
- 目前center分发消息策略是根据topic分配的worker实例列表进行随机分发。

目前存在的问题：随机策略可能导致worker负载不均衡，
优化方案：下个版本会提供多种负载均衡策略(轮训、加权轮训、加权随机)，用户可以自主选择。worker权重可以根据worker承载的消息量和负载情况（内存和cpu使用率）进行计算。

### 服务注册与发现
![avatar](images/Discovery.png)

receive/admin/center/worker服务启动时会在zookeeper上通过创建临时节点进行注册，center/worker服务可以通过获取zookeeper上/soraka/worker(center)子节点信息获取worker/center实例列表。


### receive/admin/center/worker水平扩展

- receive/admin水平扩展

![avatar](images/Scale-out.png)

admin/receive服务提供web服务，nginx提供反向代理和负载均衡（轮训或者加权轮训）功能。
- center水平扩展

当kafka消息消费过慢时，可以增加center实例提高消息消费速度。当增加center实例时，kafka消费者组重平衡机制会对topic对应的partition进行重新分配。
- worker水平扩展

重新配置topic和worker映射，将部分消息分发到新增的worker实例上。

### receive/admin/center/worker高可用
- admin单实例宕机

nginx将请求转发到正常的admin实例

- receive单实例宕机

nginx将请求转发到正常的receive实例

- center单实例宕机

当center单实例宕机时，kafka消费者组重平衡机制会对宕机center消费的partition进行重新分配，其他center实例进行接管。

- worker单实例宕机

当worker单实例宕机时，宕机worker在zookeeper上注册的临时节点会被删除，center服务监听到worker子节点变化事件，会更新内存中的worker实例列表，延时消息不会分发给宕机worker。


### worker宕机消息恢复
![avatar](images/Discovery.png)

- 当某个worker实例（x.x.x.x:30002）宕机时，center实例会收到worker子节点变化事件，然后在zookeeper上创建锁临时节点（/soraka/lock/x.x.x.x:30002），创建锁节点成功的center实例从redis中获取宕机worker备份消息，并对备份消息的延时时间进行重新计算(新的延时时间 = 消息接收时间 + 老的延时时间 - 当前时间)，最后对备份消息进行重新分发。

### 消息加时/撤回/回调失败重试策略
![avatar](images/CallbackProcess.png)

- 回调失败重试策略：最多重试5次，重试时间间隔分别为1s，10s，60s，300s，3000s，如果重试5次都失败，消息直接丢掉，业务方需要进行消息幂等校验。（目前消息回调失败不会进行重试）

