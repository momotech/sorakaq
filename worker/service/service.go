package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	statistics "sorakaq/statistics/service"

	"sorakaq/discover"

	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"sorakaq/dao"
	"sorakaq/message"
	"sorakaq/util"

	recovery "sorakaq/recovery/service"

	"github.com/momotech/timewheel"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type WorkerConsumer struct {
}

type workerService struct {
	rpcPort            int64
	workerId           string
	workerIp           string
	httpClient         *util.Client
	rpcConnPool        sync.Map
	centerAddressCache sync.Map              //所有center地址缓存
	retryMsgChan       chan message.DelayMsg //消息重试通道
}

var tw *timewheel.TimeWheel

const (
	MAX_REQ_CNT    = 3
	MIN_DELAY_TIME = 100 //ms
)

func NewWorkerService(rpcPort int64, workerId string, workerIp string) IWorkerService {
	client := &http.Client{
		Transport: &http.Transport{},
		//总超时，包含连接读写
		Timeout: 1000 * time.Millisecond,
	}
	return &workerService{
		rpcPort:      rpcPort,
		workerId:     workerId,
		workerIp:     workerIp,
		httpClient:   util.NewClient(client),
		retryMsgChan: make(chan message.DelayMsg, 1000),
	}
}

func Init(workerIp string, rpcPort int64) {

	workerId := util.GetWorkerIdByWorkerIp(workerIp)
	log.Infof("Worker ip is %s, worker id is %s, rpcport is %d", workerIp, workerId, rpcPort)
	WorkerService = NewWorkerService(rpcPort, workerId, workerIp)
	_, err := WorkerService.setCenterAddressCache()
	if err != nil {
		panic("Set center address cache err")
	}
	WorkerService.initTimeWheel()
	go WorkerService.initRpcServer()
	WorkerService.watchCenterChange()
	rand.Seed(time.Now().UnixNano())
	go WorkerService.msgRetry()
}

func (this *workerService) initTimeWheel() {
	tw = timewheel.NewTimeWheel(100, 20*time.Millisecond)
	tw.Start()
}

func (this *workerService) initRpcServer() {
	rpc.Register(new(WorkerConsumer))
	listen, err := net.Listen("tcp", util.IPToWorkerFormat(this.workerIp, this.rpcPort))
	if err != nil {
		panic(fmt.Sprintf("Init rpc server err: %v, ip: %s, port: %d", err, this.workerIp, this.rpcPort))
	}
	for {
		conn, err := listen.Accept()
		if err != nil {
			continue
		}

		go func(conn net.Conn) {
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func (this *workerService) delBackupMsg(workerId, msgId string) {
	res := recovery.RecoveryService.DelWorkerMsg(workerId, msgId, true)
	if !res {
		log.Errorf("Del redis backup msg err, workerId: %s, msgId: %s", workerId, msgId)
	}
}

//worker msg回调处理
func (this *workerService) msgCallback(msg *message.DelayMsg) error {
	log.WithFields(log.Fields{
		"receiveTime":  msg.SendTime,
		"callbackTime": uint64(time.Now().UnixNano() / 1e6),
		"delayTime":    msg.DelayTime,
		"msgId":        msg.MsgId,
	}).Infof("Before msg check")
	if !this.checkWorkerMsg(msg) {
		return nil
	}
	go this.delBackupMsg(msg.WorkerId, msg.MsgId)
	return this.httpCallBack(msg)
}

func (this *workerService) checkWorkerMsg(msg *message.DelayMsg) bool {
	//worker消息不存在
	delayMsgStr, err := recovery.RecoveryService.GetWorkerMsgByMsgId(msg.WorkerId, msg.MsgId)
	if err != nil {
		go func(msg *message.DelayMsg) {
			this.retryMsgChan <- *msg
		}(msg)
		return false
	}
	if delayMsgStr == "" {
		log.Infof("Worker msg is not exist, workerId: %s, msgId: %s", msg.WorkerId, msg.MsgId)
		return false
	}

	delayMsg := new(message.DelayMsg)
	err = json.Unmarshal([]byte(delayMsgStr), &delayMsg)
	if err != nil {
		log.Errorf("Unmarshal msg err, msg:%s, err:%v", delayMsg, err)
		return true
	}
	go this.delBackupMsg(msg.WorkerId, msg.MsgId)
	if delayMsg.MsgType == message.NORMAO_DELAY_MSG {
		return true
	}
	if delayMsg.MsgType == message.CANCEL_MSG {
		log.Infof("Msg cancel, msgId: %s", msg.MsgId)
		return false
	}
	if delayMsg.MsgType == message.ADD_TIME_MSG { //加时消息
		this.addTimeMsgCallback(msg, delayMsg.DelayTime)
		return false
	}
	return true
}

func (this *workerService) addTimeMsgCallback(msg *message.DelayMsg, addTime uint64) {
	request := new(message.Request)
	request.Msg = *msg
	request.Msg.DelayTime = addTime
	excludeCenterAddress := make([]string, 0)
	for reqCnt := 1; reqCnt <= MAX_REQ_CNT; reqCnt++ {
		//发送给center重新调度
		centerAddress, err := this.getOneCenterAddress(excludeCenterAddress)
		if err != nil {
			log.Errorf("Get center address err: %v", err)
			break
		}
		log.Infof("Msg delay, center callback, msgId: %s, delayTime: %d, center: %v, reqCnt: %d", msg.MsgId, addTime, centerAddress, reqCnt)
		err = this.sendMsg(centerAddress, request)
		if err != nil {
			excludeCenterAddress = append(excludeCenterAddress, centerAddress)
			continue
		}
		return
	}
	//center address不存在或者请求center 3次失败，本机回调处理
	this.process(&request.Msg)
	log.Infof("Msg delay, local host callback, msgId: %s, delayTime: %d", msg.MsgId, addTime)
	return
}

func (this *workerService) httpCallBack(msg *message.DelayMsg) error {
	requestURL := msg.Body.URL
	requestType := msg.RequestType
	args := msg.Body.Params
	timeout := msg.Ttr
	currentTime := uint64(time.Now().UnixNano() / 1e6)
	log.WithFields(log.Fields{
		"receiveTime":  msg.SendTime,
		"callbackTime": currentTime,
		"elapsedTime":  currentTime - msg.SendTime,
		"delayTime":    msg.DelayTime,
		"msgId":        msg.MsgId,
		"requestUrl":   requestURL,
		"requestType":  requestType,
		"args":         args,
		"timeout":      timeout,
	}).Infof("Before msg callback")
	if requestType != "GET" && requestType != "POST" {
		log.Errorf("Http request type err, msgId:%s, requestType:%s", msg.MsgId, requestType)
		return errors.New("Http request type err")
	}
	this.httpClient.Timeout = time.Duration(timeout) * time.Millisecond
	req, err := http.NewRequest(requestType, requestURL, bytes.NewReader(([]byte)(args)))
	if err != nil {
		log.Errorf("Http new request err, msgId: %s: err: %v", msg.MsgId, err)
		return err
	}
	req.Header.Set("Content-type", "application/json;charset='utf-8'")
	response := &message.Response{}
	startTime := uint64(time.Now().UnixNano() / 1e6)
	err = this.httpClient.DoAsJson(req, response)
	endTime := uint64(time.Now().UnixNano() / 1e6)
	res := this.checkCallbackResult(msg, response, err)
	if currentTime > msg.SendTime+msg.DelayTime {
		elapsedTime := currentTime - msg.SendTime - msg.DelayTime
		statistics.StatisticsService.RecordElapsedTime(msg.TopicName, msg.MsgId, elapsedTime, endTime-startTime)
	}
	return res
}

func (this *workerService) checkCallbackResult(msg *message.DelayMsg, response *message.Response, err error) error {
	if err != nil {
		log.Errorf("Http msg callback err, msgId: %s: err: %v", msg.MsgId, err)
		return err
	}
	log.WithFields(log.Fields{
		"msgId":    msg.MsgId,
		"response": response,
	}).Infof("After msg callback")
	if !(response.Result) {
		log.Errorf("MsgId: %s, response result is false, em: %s, ec: %v", msg.MsgId, response.Em, response.Ec)
		return errors.New("Response result is false")
	}
	return nil
}

//延时消息消费
func (this *WorkerConsumer) ConsumerDelayMsg(req message.Request, res *message.Response) error {
	WorkerService.process(&req.Msg)
	res.Result = true
	return nil
}

func (this *workerService) process(delayMsg *message.DelayMsg) error {
	log.WithFields(log.Fields{
		"msgId":     delayMsg.MsgId,
		"delayTime": delayMsg.DelayTime,
	}).Infof("Receive delay time")
	if delayMsg.DelayTime < MIN_DELAY_TIME {
		go this.msgCallback(delayMsg)
		return nil
	}
	tw.Add(time.Duration(delayMsg.DelayTime)*time.Millisecond, func() {
		this.msgCallback(delayMsg)
	})
	return nil
}

//监听worker变化
func (this *workerService) watchCenterChange() error {
	ech, err := dao.ZkDao.WatchChildNode(util.TmpCenterNode())
	if err != nil {
		log.Errorf("Watch children error, %v", err)
		return err
	}
	go this.watchCenterChildChange(ech)
	return nil
}

//监听worker子节点变化
func (this *workerService) watchCenterChildChange(watch <-chan zk.Event) error {
	event := <-watch
	if event.Type == zk.EventNodeChildrenChanged {
		oldCenterAddressCache := this.getCenterAddressCache()
		newCenterAddressCache, err := this.setCenterAddressCache()
		if err != nil {
			this.watchCenterChange()
			return err
		}
		//获取宕机worker消息，重新分发
		diffCenterAddress := util.Difference(oldCenterAddressCache, newCenterAddressCache)
		log.Infof("Down center address : %v", diffCenterAddress)
		if len(diffCenterAddress) == 0 {
			this.watchCenterChange()
			return nil
		}
		go this.closeRpcConn(diffCenterAddress)
	}
	this.watchCenterChange()
	return nil
}

//获取center地址缓存
func (this *workerService) getCenterAddressCache() []string {
	workerCache, ok := this.centerAddressCache.Load(util.CENTER_ADDRESS_CACHE_KEY)
	if ok == false {
		log.Errorf("Get center address cache failed")
		return nil
	}
	return workerCache.([]string)
}

//设置center地址缓存
func (this *workerService) setCenterAddressCache() ([]string, error) {
	childrens, err := this.getAllCenterAddress()
	if err != nil {
		return nil, err
	}
	log.Infof("Set center address cache, %v", childrens)
	this.centerAddressCache.Store(util.CENTER_ADDRESS_CACHE_KEY, childrens)
	return childrens, err
}

//获取所有的center地址
func (this *workerService) getAllCenterAddress() ([]string, error) {
	childrens, err := discover.DiscoverClient.DiscoverServices(util.CENTER_SERVICE)
	if err != nil {
		log.Errorf("Get center list err, %v", err)
		return nil, err
	}
	return childrens, nil
}

func (this *workerService) closeRpcConn(centers []string) {
	for _, address := range centers {
		connCache, ok := this.rpcConnPool.Load(address)
		if ok {
			connCache.(*rpc.Client).Close()
			this.rpcConnPool.Delete(address)
		}
	}
}

//消息发送到worker
func (this *workerService) sendMsg(address string, req *message.Request) error {
	connCache, ok := this.rpcConnPool.Load(address)
	var client *rpc.Client
	if false == ok {
		//"127.0.0.1:30000"
		var err error
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err != nil {
			log.Errorf("Error get repClient, address: %s, req: %v, err: %v", address, req, err)
			return err
		}
		client = jsonrpc.NewClient(conn)
		this.rpcConnPool.Store(address, client)
		log.Infof("Add center conn pool, %s", address)
	} else {
		client = connCache.(*rpc.Client)
	}
	log.Infof("Req center address, %s, msgId: %s", address, req.Msg.MsgId)

	done := make(chan error, 1)
	go func() {
		var res message.Response
		err := client.Call("CenterConsumer.ConsumerDelayMsg", req, &res)
		done <- err
	}()
	timeout := 5 * time.Second
	centers := []string{address}
	select {
	case <-time.After(timeout):
		log.Errorf("Center consumer delay msg timeout, %v", timeout)
		this.closeRpcConn(centers)
		return errors.New("Send msg to center timeout")
	case err := <-done:
		if err != nil {
			this.closeRpcConn(centers)
			return err
		}
	}
	return nil
}

//获取center地址
func (this *workerService) getOneCenterAddress(excludeCenterAddress []string) (string, error) {
	centerCache := this.getCenterAddressCache()
	log.Debugf("CenterCacheData: %v", centerCache)
	if centerCache == nil || len(centerCache) == 0 {
		return "", errors.New("all center address cache is empty")
	}
	diffCenters := util.Difference(centerCache, excludeCenterAddress)
	if len(diffCenters) == 0 {
		diffCenters = centerCache
	}
	return this.getOneActiveCenterAddress(diffCenters), nil
}

//获取一个活跃的center地址
func (this *workerService) getOneActiveCenterAddress(centers []string) string {
	n := rand.Intn(len(centers))
	return centers[n]
}

func (this *workerService) Stop() {
	centerCache := this.getCenterAddressCache()
	if centerCache == nil || len(centerCache) == 0 {
		return
	}
	this.closeRpcConn(centerCache)
}

func (this *workerService) msgRetry() {
	for {
		time.Sleep(10 * time.Millisecond)
		msg, ok := <-this.retryMsgChan
		if !ok {
			time.Sleep(100 * time.Millisecond)
			log.Debugf("Retry msg not exist")
			continue
		}
		log.WithFields(log.Fields{
			"msgId": msg.MsgId,
		}).Infof("Msg retry")
		go this.msgCallback(&msg)
	}
}
