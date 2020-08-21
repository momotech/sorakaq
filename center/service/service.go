package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc/jsonrpc"
	"strings"
	"sync"
	"time"

	"sorakaq/center/entity"
	dao "sorakaq/dao"
	"sorakaq/discover"
	"sorakaq/message"
	recovery "sorakaq/recovery/service"
	"sorakaq/util"

	"net/rpc"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type centerService struct {
	kafkaConfig             *entity.KafkaConfig
	kafkaTopics             []string
	rpcConnPool             sync.Map
	rpcPort                 int64
	centerIp                string
	topicWorkerAddressCache sync.Map             //topic对应的worker地址缓存
	workerAddressCache      sync.Map             //所有worker地址缓存
	retryMsgChan            chan message.Request //消息重试通道
}

const (
	WORKER_ADDRESS_EXPIRE                  = 60  //60秒重新取worker address
	PULL_OFFLINE_WORKER_MSG_NUM_EVERY_TIME = 500 //每次拉取下线worker消息个数
	MAX_REQ_CNT                            = 3   //每个消息请求worker最大次数
)

type Consumer struct {
	ready chan bool
}

type CenterConsumer struct {
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Infof("Message claimed: key = %s, value = %v, topic = %s, partition = %v, offset = %v", string(message.Key), string(message.Value), message.Topic, message.Partition, message.Offset)
		go func(message *sarama.ConsumerMessage) {
			err := CenterService.msgCallback(message.Value, false)
			if err != nil {
				log.Errorf("msg call back err: %v", err)
				return
			}
			session.MarkMessage(message, "")
		}(message)
	}
	return nil
}

func Init(brokerList string, topicPrefix string, centerIp string, rpcPort int64) {
	log.Infof("CenterIp : %s, rpcPort: %d", centerIp, rpcPort)
	CenterService = NewCenterService(brokerList, topicPrefix, rpcPort, centerIp)
	_, err := CenterService.setWorkerAddressCache()
	if err != nil {
		panic("Set worker address cache err")
	}
	go CenterService.initRpcServer()
	CenterService.watchTopicChange()
	CenterService.watchWorkerChange()
	rand.Seed(time.Now().UnixNano())
	go CenterService.msgRetry()
}

func (this *centerService) initRpcServer() {
	rpc.Register(new(CenterConsumer)) // 注册rpc服务
	listen, err := net.Listen("tcp", util.IPToWorkerFormat(this.centerIp, this.rpcPort))
	if err != nil {
		panic(fmt.Sprintf("Init rpc server err: %v, ip: %s, port: %d", err, this.centerIp, this.rpcPort))
	}
	for {
		conn, err := listen.Accept() // 接收客户端连接请求
		if err != nil {
			continue
		}

		go func(conn net.Conn) { // 并发处理客户端请求
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func NewCenterService(brokerList string, topicPrefix string, rpcPort int64, centerIp string) ICenterService {
	config := InitKafkaConf(brokerList, topicPrefix)
	topics := make([]string, 0)
	return &centerService{
		kafkaConfig:  config,
		kafkaTopics:  topics,
		rpcPort:      rpcPort,
		centerIp:     centerIp,
		retryMsgChan: make(chan message.Request, 1000),
	}
}

func InitKafkaConf(brokerList string, topicPrefix string) *entity.KafkaConfig {
	_, err := sarama.ParseKafkaVersion("2.3.0")
	if err != nil {
		log.Errorf("Error parsing Kafka version: %v", err)
	}
	kafkaConfig := &entity.KafkaConfig{}
	kafkaConfig.Config = sarama.NewConfig()
	kafkaConfig.Config.Metadata.RefreshFrequency = 10 * time.Second
	kafkaConfig.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Config.Version = sarama.V0_11_0_2
	kafkaConfig.Config.Consumer.Return.Errors = true
	kafkaConfig.BrokerServers = brokerList
	kafkaConfig.TopicPrefix = topicPrefix
	return kafkaConfig
}

//启动center
func (this *centerService) Start() {
	kafkaTopics, err := this.getAllDelayTopics()
	if err != nil {
		log.Errorf("Start service err, err: %v", err)
		return
	}
	this.kafkaTopics = kafkaTopics
	log.Debugf("Kafka topics: %v", kafkaTopics)
	this.createConsumer(kafkaTopics)
}

//创建消费者
func (this *centerService) createConsumer(topics []string) {
	for k, _ := range topics {
		go this.createConsumerGroup(topics[k : k+1])
	}
}

//消费kafka
func (this *centerService) createConsumerGroup(topics []string) {
	consumer := Consumer{
		ready: make(chan bool, 0),
	}
	//sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(this.kafkaConfig.BrokerServers, ","), topics[0], this.kafkaConfig.Config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	defer client.Close()

	go func() {
		for err := range client.Errors() {
			log.Errorf("Error: %s", err.Error())
		}
	}()

	go func() {
		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				log.Errorf("Ctx err: %v", ctx.Err())
				return
			}
			consumer.ready = make(chan bool, 0)
		}
	}()
	select {
	case <-ctx.Done():
		log.Println("Terminating: context cancelled")
	}

	<-consumer.ready
	cancel()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

//获取所有延时topic
func (this *centerService) getAllDelayTopics() ([]string, error) {
	childrens, err := dao.ZkDao.GetChildrenNode(util.KAFKA_TOPIC_PATH)
	if err != nil {
		log.Errorf("Get kafka topic err, %v", err)
		return nil, err
	}
	var topics []string
	for _, topic := range childrens {
		if this.isDelayTopic(topic) {
			topics = append(topics, topic)
		}
	}
	log.Infof("Delay topics: %v", topics)
	return topics, nil
}

//校验是否是延时topic
func (this *centerService) isDelayTopic(topic string) bool {
	if topic == "__consumer_offsets" {
		return false
	}
	if this.kafkaConfig.TopicPrefix == "" || strings.HasPrefix(topic, this.kafkaConfig.TopicPrefix) {
		return true
	}
	return false
}

//同步消息到redis
func (this *centerService) backupMsg(delayMsg *message.DelayMsg) bool {
	detail, _ := json.Marshal(delayMsg)
	res := recovery.RecoveryService.AddWorkerMsg(delayMsg.WorkerId, delayMsg.MsgId, int64(delayMsg.DelayTime+delayMsg.SendTime), string(detail))
	if !res {
		log.Errorf("Sync msg to redis err, msg: %s", string(detail))
	}
	return res
}

//消息发送到worker
func (this *centerService) sendMsg(address string, req *message.Request) error {
	connCache, ok := this.rpcConnPool.Load(address)
	var client *rpc.Client
	if false == ok {
		//"127.0.0.1:30000"
		var err error
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err != nil {
			log.WithFields(log.Fields{
				"msgId":   req.Msg.MsgId,
				"address": address,
				"err":     err,
			}).Error("Error get repClient")
			return err
		}
		client = jsonrpc.NewClient(conn)
		this.rpcConnPool.Store(address, client)
		log.WithFields(log.Fields{
			"msgId":   req.Msg.MsgId,
			"address": address,
		}).Info("Add center conn pool")
	} else {
		client = connCache.(*rpc.Client)
	}
	log.WithFields(log.Fields{
		"msgId":   req.Msg.MsgId,
		"address": address,
	}).Info("Req worker address")

	done := make(chan error, 1)
	go func() {
		var res message.Response
		err := client.Call("WorkerConsumer.ConsumerDelayMsg", req, &res)
		done <- err
	}()
	timeout := 5 * time.Second
	workers := []string{address}
	select {
	case <-time.After(timeout):
		log.WithFields(log.Fields{
			"msgId":   req.Msg.MsgId,
			"address": address,
			"time":    timeout,
		}).Error("Send msg timeout")
		this.closeRpcConn(workers)
		return errors.New("Send msg to worker timeout")
	case err := <-done:
		if err != nil {
			this.closeRpcConn(workers)
			return err
		}
	}

	return nil
}

//kafka消息回调
func (this *centerService) msgCallback(msg []byte, isDownWorkerMsg bool) error {
	log.Debugf("Msg : %v, isDownWorkerMsg: %v", string(msg), isDownWorkerMsg)
	request := new(message.Request)
	err := json.Unmarshal(msg, &request.Msg)
	if err != nil {
		log.Errorf("Unmarshal msg err, msg:%s, err:%v", msg, err)
		return err
	}
	if isDownWorkerMsg {
		currentTime := uint64(time.Now().UnixNano() / 1e6)
		//重新计算延时时间
		if request.Msg.DelayTime+request.Msg.SendTime > currentTime {
			request.Msg.DelayTime = request.Msg.DelayTime + request.Msg.SendTime - currentTime
		} else {
			request.Msg.DelayTime = 0
		}
	}
	log.Debugf("Request: %v", request)
	return this.msgCallbackWorker(request)
}

func (this *centerService) msgCallbackWorker(request *message.Request) error {
	topic := request.Msg.TopicName
	excludeWorkerAddress := make([]string, 0)
	for reqCnt := 1; reqCnt <= MAX_REQ_CNT; reqCnt++ {
		workerAddress, err := this.getOneWorkerAddress(topic, excludeWorkerAddress)
		if err != nil {
			return err
		}
		request.Msg.WorkerId = util.GetWorkerIdByWorkerIp(strings.Split(workerAddress, ":")[0])
		log.WithFields(log.Fields{
			"msgId":    request.Msg.MsgId,
			"address":  workerAddress,
			"workerId": request.Msg.WorkerId,
		}).Info("Before backup msg ")

		if !this.backupMsg(&request.Msg) {
			log.WithFields(log.Fields{
				"msgId":    request.Msg.MsgId,
				"address":  workerAddress,
				"workerId": request.Msg.WorkerId,
			}).Errorf("Sync msg to redis err")
			go func(request *message.Request) {
				this.retryMsgChan <- *request
			}(request)
			return errors.New(fmt.Sprintf("Sync msg to redis err, msgId: %s", request.Msg.MsgId))
		}
		log.WithFields(log.Fields{
			"msgId":    request.Msg.MsgId,
			"address":  workerAddress,
			"workerId": request.Msg.WorkerId,
			"reqCnt":   reqCnt,
		}).Info("Before send msg ")
		err = this.sendMsg(workerAddress, request)
		if err != nil {
			go this.delBackupMsg(request.Msg.WorkerId, request.Msg.MsgId, true)
			excludeWorkerAddress = append(excludeWorkerAddress, workerAddress)
			continue
		}
		log.WithFields(log.Fields{
			"msgId":    request.Msg.MsgId,
			"address":  workerAddress,
			"workerId": request.Msg.WorkerId,
			"reqCnt":   reqCnt,
		}).Info("After send msg ")
		return nil
	}
	return errors.New("Msg call back worker failed, msgId: " + request.Msg.MsgId)
}

//获取worker地址
func (this *centerService) getOneWorkerAddress(topic string, excludeWorkerAddress []string) (string, error) {
	workerCache := this.getWorkerAddressCache()
	if workerCache == nil || len(workerCache) == 0 {
		return "", errors.New("All worker address cache is empty")
	}
	diffWorkers := util.Difference(workerCache, excludeWorkerAddress)
	if len(diffWorkers) == 0 {
		diffWorkers = workerCache
	}
	topicWorkerCache := this.getTopicWorkerAddressCache(topic)
	if topicWorkerCache != nil {
		return this.getOneActiveWorkerAddress(topicWorkerCache, diffWorkers, topic)
	}

	workers, err := this.getZKTopicWorkerAddresss(topic)
	if err != nil {
		log.Warnf("Get zk topic worker address failed, %v", err)
	}

	var timestamp = time.Now().Unix()
	this.topicWorkerAddressCache.Store(topic, map[string]interface{}{
		"expire":                      timestamp + WORKER_ADDRESS_EXPIRE, //更新过期时间
		util.WORKER_ADDRESS_CACHE_KEY: workers,
	})
	log.Infof("Set topic worker address cache, topic: %s, workers:%v", topic, workers)
	return this.getOneActiveWorkerAddress(workers, diffWorkers, topic)
}

//获取一个活跃的worker地址
func (this *centerService) getOneActiveWorkerAddress(topicWorkers []string, allWorkers []string, topic string) (string, error) {
	if topicWorkers == nil {
		log.Warnf("Topic worker address is not exist, topic: %s", topic)
		//如果topic分配的worker不存在，在所有活跃的worker中随机取一个
		n := rand.Intn(len(allWorkers))
		return allWorkers[n], nil
	}
	activeWorkers := util.Intersect(topicWorkers, allWorkers)
	if len(activeWorkers) == 0 {
		log.Warnf("Active topic worker address is not exist, topic: %s", topic)
		//如果topic分配的活跃worker不存在，在所有活跃的worker中随机取一个
		n := rand.Intn(len(allWorkers))
		return allWorkers[n], nil
	}
	n := rand.Intn(len(activeWorkers))
	return activeWorkers[n], nil
}

//获取zk上topic绑定的worker地址
func (this *centerService) getZKTopicWorkerAddresss(topic string) ([]string, error) {
	path := util.TopicNode(topic)
	exists, _, err := dao.ZkDao.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []string{}, errors.New("Zk node is not exist :" + path)
	}
	var value []byte
	value, _, err = dao.ZkDao.Get(path)
	if err != nil {
		return []string{}, errors.New(fmt.Sprintf("Get zk node err, err:%v, path:%s", err, path))
	}
	topicWorkerInfo := new(entity.TopicZkInfo)
	err = json.Unmarshal(value, &topicWorkerInfo)
	if err != nil {
		return []string{}, errors.New(fmt.Sprintf("Unmarshal msg err, msg:%s, err:%v", value, err))
	}
	return topicWorkerInfo.Workers, nil
}

//监听kafka topic变化
func (this *centerService) watchTopicChange() error {
	ech, err := dao.ZkDao.WatchChildNode(util.KAFKA_TOPIC_PATH)
	if err != nil {
		log.Errorf("Watch children error, %v", err)
		return err
	}
	go this.watchTopicChildChange(ech)
	return nil
}

//监听kafka topic子节点变化
func (this *centerService) watchTopicChildChange(watch <-chan zk.Event) error {
	event := <-watch
	if event.Type == zk.EventNodeChildrenChanged {
		newTopics, err := this.getAllDelayTopics()
		if err != nil {
			log.Errorf("Get kafka topics err, %v", err)
			this.watchTopicChange()
			return err
		}
		addTopics := util.Difference(newTopics, this.kafkaTopics)
		log.Debugf("AddTopics : %v", addTopics)
		if len(addTopics) > 0 {
			this.createConsumer(addTopics)
			this.kafkaTopics = util.Union(newTopics, this.kafkaTopics)
			log.Debugf("Kafka topics update: %v", this.kafkaTopics)
		}
		this.watchTopicChange()
	}
	return nil
}

//监听worker变化
func (this *centerService) watchWorkerChange() error {
	ech, err := dao.ZkDao.WatchChildNode(util.TmpWorkerNode())
	if err != nil {
		log.Errorf("Watch children error, %v", err)
		return err
	}
	go this.watchWorkerChildChange(ech)
	return nil
}

//监听worker子节点变化
func (this *centerService) watchWorkerChildChange(watch <-chan zk.Event) error {
	event := <-watch
	if event.Type == zk.EventNodeChildrenChanged {
		oldWorkerAddressCache := this.getWorkerAddressCache()
		newWorkerAddressCache, err := this.setWorkerAddressCache()
		if err != nil {
			this.watchWorkerChange()
			return err
		}
		//获取宕机worker消息，重新分发
		diffWorkerAddress := util.Difference(oldWorkerAddressCache, newWorkerAddressCache)
		log.Infof("Down worker address : %v", diffWorkerAddress)
		if len(diffWorkerAddress) == 0 {
			this.watchWorkerChange()
			return nil
		}
		go this.closeRpcConn(diffWorkerAddress)
		go this.reSendDownWorkerMsg(diffWorkerAddress)
	}
	this.watchWorkerChange()
	return nil
}

//获取宕机worker消息，重新分发
func (this *centerService) reSendDownWorkerMsg(downWorkerAddress []string) {
	for _, address := range downWorkerAddress {
		lockChildName := "downworker" + address
		err := dao.ZkDao.Lock(lockChildName)
		if err != nil {
			log.Infof("Create lock failed, address: %s, err: %v", address, err)
			continue
		}
		workerId := util.GetWorkerIdByWorkerIp(strings.Split(address, ":")[0])
		log.WithFields(log.Fields{
			"address":  address,
			"workerId": workerId,
		}).Info("Start pull down worker msg")
		this.pullDownWorkerMsgCallback(workerId)
		dao.ZkDao.Unlock(lockChildName)
	}
}

func (this *centerService) pullDownWorkerMsgCallback(workerId string) {
	for {
		workerMsgs, err := recovery.RecoveryService.GetWorkerMsgByShard(workerId, 0, PULL_OFFLINE_WORKER_MSG_NUM_EVERY_TIME-1)
		if err != nil {
			log.Errorf("Get down worker msg failed, workerId : %s, err : %v", workerId, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		msgCnt := len(workerMsgs)
		if msgCnt == 0 {
			log.WithFields(log.Fields{
				"workerId": workerId,
				"msgCnt":   msgCnt,
			}).Info("Complete pull down worker msg")
			break
		}
		log.WithFields(log.Fields{
			"workerId": workerId,
			"msgCnt":   msgCnt,
		}).Info("Pull down worker msg")
		var wg sync.WaitGroup
		for msgId, msg := range workerMsgs {
			if msg == "" {
				//删除宕机的worker 备份消息
				this.delBackupMsg(workerId, msgId, false)
				continue
			}
			wg.Add(1)
			go func(msgId string, workerMsg string) {
				callbackRes := this.msgCallback([]byte(workerMsg), true)
				if callbackRes == nil {
					//删除宕机的worker 备份消息
					this.delBackupMsg(workerId, msgId, false)
				}
				wg.Done()
			}(msgId, msg)
		}
		wg.Wait()
		time.Sleep(20 * time.Millisecond)
	}
}

//设置worker地址缓存
func (this *centerService) setWorkerAddressCache() ([]string, error) {
	childrens, err := this.getAllWorkerAddress()
	if err != nil {
		return nil, err
	}
	log.Infof("Set worker address cache, %v", childrens)
	this.workerAddressCache.Store(util.WORKER_ADDRESS_CACHE_KEY, childrens)
	return childrens, err
}

//获取topic对应的worker地址缓存
func (this *centerService) getTopicWorkerAddressCache(topic string) []string {
	topicWorkerMapCache, ok := this.topicWorkerAddressCache.Load(topic)
	if ok == false {
		log.Infof("Get topic map worker address cache failed, topic :%s", topic)
		return nil
	}
	var timestamp = time.Now().Unix()
	log.Debugf("Topic: %s, cacheData: %v, current time: %v", topic, topicWorkerMapCache, timestamp)
	if topicWorkerMapCache != nil && topicWorkerMapCache.(map[string]interface{})["expire"].(int64) > timestamp {
		return topicWorkerMapCache.(map[string]interface{})[util.WORKER_ADDRESS_CACHE_KEY].([]string)
	}
	return nil
}

//获取worker地址缓存
func (this *centerService) getWorkerAddressCache() []string {
	workerCache, ok := this.workerAddressCache.Load(util.WORKER_ADDRESS_CACHE_KEY)
	if ok == false {
		log.Errorf("Get worker address cache failed")
		return nil
	}
	return workerCache.([]string)
}

//获取所有的worker地址
func (this *centerService) getAllWorkerAddress() ([]string, error) {
	childrens, err := discover.DiscoverClient.DiscoverServices(util.WORKER_SERVICE)
	if err != nil {
		log.Errorf("Get worker list err, %v", err)
		return nil, err
	}
	return childrens, nil
}

func (this *centerService) closeRpcConn(workers []string) {
	for _, address := range workers {
		connCache, ok := this.rpcConnPool.Load(address)
		if ok {
			connCache.(*rpc.Client).Close()
			this.rpcConnPool.Delete(address)
		}
	}
}

func (this *centerService) Stop() {
	workerCache := this.getWorkerAddressCache()
	if workerCache == nil || len(workerCache) == 0 {
		return
	}
	this.closeRpcConn(workerCache)
}

//延时消息消费
func (this *CenterConsumer) ConsumerDelayMsg(req message.Request, res *message.Response) error {
	log.Debugf("Consumer delay msg, req: %v", req)
	return CenterService.msgCallbackWorker(&req)
}

func (this *centerService) delBackupMsg(workerId string, msgId string, isDelWorkerIdMap bool) {
	res := recovery.RecoveryService.DelWorkerMsg(workerId, msgId, isDelWorkerIdMap)
	if !res {
		log.WithFields(log.Fields{
			"msgId":    msgId,
			"workerId": workerId,
		}).Error("Del worker redis backup msg err")
	}
}

//消息重试
func (this *centerService) msgRetry() {
	for {
		time.Sleep(10 * time.Millisecond)
		req, ok := <-this.retryMsgChan
		if !ok {
			time.Sleep(100 * time.Millisecond)
			log.Debugf("Retry msg not exist")
			continue
		}
		log.WithFields(log.Fields{
			"msgId": req.Msg.MsgId,
		}).Infof("Msg retry")
		go this.msgCallbackWorker(&req)
	}
}
