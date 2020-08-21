package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	zkdao "sorakaq/dao"

	"sorakaq/admin/entity"
	"sorakaq/discover"
	"sorakaq/util"

	"sorakaq/admin/dao"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/Shopify/sarama"

	log "github.com/sirupsen/logrus"
)

type ZkHandleType int

const (
	_ ZkHandleType = iota
	ADDNODE
	REMOVENODE
)

type topicService struct {
	m                  *sync.Mutex
	workerAddressCache sync.Map //所有worker地址缓存
	TopicPrefix        string
}

func NewTopicService(TopicPrefix string) ITopicService {
	return &topicService{m: new(sync.Mutex), TopicPrefix: TopicPrefix}
}

func (ts *topicService) Create(topic *entity.Topic) (string, error) {
	path := util.TopicNode(topic.TopicName)
	exists, _, err := zkdao.ZkDao.Exists(path)
	if err != nil || exists {
		log.Errorf("Create Topic Error:%v,path:%s", err, path)
		return "", errors.New(fmt.Sprintf("Create Topic Error: %v, path: %s", err, path))
	}

	topicZkInfo := &entity.TopicZkInfo{}
	topicZkInfo.TopicName = topic.TopicName
	topicZkInfo.Business = topic.Business
	topicZkInfo.WorkerNum = topic.WorkerNum

	topicZkInfo.Workers, err = ts.getAssignedWorkers(topic.WorkerNum, make([]string, 0))
	log.WithFields(log.Fields{
		"topicName":       topic.TopicName,
		"WorkerNum":       topic.WorkerNum,
		"assignedWorkers": topicZkInfo.Workers,
	}).Info("assign worker")
	if err != nil {
		return "", err
	}

	data, _ := json.Marshal(topicZkInfo)
	_, err = zkdao.ZkDao.Create(path, data, 0)
	if err != nil {
		log.Errorf("Create Error: %v, path: %s, data: %s", err, path, string(data))
		return "", err
	}

	ts.updateIPTopic(ADDNODE, topicZkInfo.Workers, topic.TopicName)

	// kafka create topic
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = 5
	topicDetail.ReplicationFactor = 1
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic.TopicName] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}
	_, err = dao.KafkaDao.CreateTopics(&request)
	if err != nil {
		log.Errorf("Create kafka topic err, %v, %v", request, err)
	}
	return "", nil
}

func (ts *topicService) Update(topic *entity.Topic) error {
	return ts.updateTopicWorkerMap(topic.TopicName, "", topic.WorkerNum)
}

func (ts *topicService) GetList() ([]string, error) {
	childrens, err := zkdao.ZkDao.GetChildrenNode(util.KAFKA_TOPIC_PATH)
	if err != nil {
		log.Errorf("Get kafka topic err, %v", err)
		return nil, err
	}
	var topics []string
	for _, topic := range childrens {
		if ts.isDelayTopic(topic) {
			topics = append(topics, topic)
		}
	}
	return topics, nil
}

//校验是否是延时topic
func (ts *topicService) isDelayTopic(topic string) bool {
	if topic == "__consumer_offsets" {
		return false
	}
	if ts.TopicPrefix == "" || strings.HasPrefix(topic, ts.TopicPrefix) {
		return true
	}
	return false
}

func (ts *topicService) Get(topic string) (entity.TopicZkInfo, error) {
	path := util.TopicNode(topic)

	TopicZkInfo := entity.TopicZkInfo{TopicName: topic}
	exists, _, err := zkdao.ZkDao.Exists(path)
	if err != nil || !exists {
		log.Errorf("Topic Exists Error path:%s", path)
		return TopicZkInfo, err
	}

	topicInfo, stat, err := zkdao.ZkDao.Get(path)
	if err != nil {
		log.Errorf("Get topic info Error path:%s", path)
		return TopicZkInfo, err
	}
	TopicZkInfo.CreateTime = time.Unix(stat.Ctime/1000, 0).Format("2006-01-02 15:04:05")
	err = json.Unmarshal(topicInfo, &TopicZkInfo)
	return TopicZkInfo, err
}

func (ts *topicService) getAssignedWorkers(workerNum int, excludeWorkerAddress []string) ([]string, error) {
	workers := ts.getWorkerAddressCache()
	if workers == nil || len(workers) < 1 {
		return nil, errors.New("Worker instance is not exist")
	}

	workers = util.Difference(workers, excludeWorkerAddress)
	totalWorkerNum := len(workers)
	if workerNum <= 0 {
		workerNum = 1
	} else if workerNum > totalWorkerNum {
		workerNum = totalWorkerNum
	}

	rand.Seed(time.Now().UnixNano())
	assginedWorkers := make([]string, workerNum)
	for i := 0; i < workerNum; i++ {
		r := rand.Intn(totalWorkerNum)
		assginedWorkers[i] = workers[r]
		workers = append(workers[0:r], workers[r+1:]...)
		totalWorkerNum = len(workers)
	}
	return assginedWorkers, nil
}

func (ts *topicService) updateIPTopic(handle ZkHandleType, IPS []string, topic string) (bool, error) {
	if len(IPS) == 0 {
		return false, errors.New("IPS is empty")
	}

	exists, _, _ := zkdao.ZkDao.Exists(util.IPTopicNode())
	if !exists {
		zkdao.ZkDao.Create(util.IPTopicNode(), []byte(""), 0)
	}

	for _, worker := range IPS {
		path := util.IPTopicsNode(worker)
		IPTopics := entity.IPTopics{}
		exists, stat, _ := zkdao.ZkDao.Exists(path)
		if !exists {
			_, err := zkdao.ZkDao.Create(path, []byte(""), 0)
			if err != nil {
				log.Errorf("Create Iptopic node Error:%s", err)
				continue
			}
		} else {
			existsData, _, _ := zkdao.ZkDao.Get(path)
			_ = json.Unmarshal(existsData, &IPTopics)
		}
		if handle == ADDNODE {
			IPTopics.Topics = util.Union(IPTopics.Topics, []string{topic})
		} else if handle == REMOVENODE {
			key := util.GetIPPos(IPTopics.Topics, topic)
			if key > -1 {
				IPTopics.Topics = append(IPTopics.Topics[:key], IPTopics.Topics[key+1:]...)
			}
		}

		data, _ := json.Marshal(IPTopics)
		b, err := zkdao.ZkDao.Set(path, data, stat.Version)
		if err != nil {
			log.Errorf("UpdateIPTopic Set Error: %s, path: %s, data: %s, b: %t", err, path, data, b)
		}
	}

	return true, nil
}

func Init() {
	_, err := TopicService.setWorkerAddressCache()
	if err != nil {
		panic("Set worker address cache err")
	}
	TopicService.watchWorkerChange()
}

//监听worker变化
func (this *topicService) watchWorkerChange() error {
	ech, err := zkdao.ZkDao.WatchChildNode(util.TmpWorkerNode())
	if err != nil {
		log.Errorf("Watch children error, %v", err)
		return err
	}
	go this.watchWorkerChildChange(ech)
	return nil
}

//监听worker子节点变化
func (this *topicService) watchWorkerChildChange(watch <-chan zk.Event) error {
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
		log.Infof("Down worker address: %v", diffWorkerAddress)
		if len(diffWorkerAddress) == 0 {
			this.watchWorkerChange()
			return nil
		}

		for _, address := range diffWorkerAddress {
			lockChildName := "downworkertopic" + address
			err = zkdao.ZkDao.Lock(lockChildName)
			if err != nil {
				log.Infof("Create lock failed, address: %s, err: %v", address, err)
				continue
			}
			log.Infof("Get lock, %s", lockChildName)
			this.removeTopicIP(address)
			zkdao.ZkDao.Unlock(lockChildName)
			log.Infof("Unlock, %s", lockChildName)
		}
	}
	this.watchWorkerChange()
	return nil
}

func (ts *topicService) removeTopicIP(ipPortStr string) error {

	path := util.IPTopicsNodeWithPortV2(ipPortStr)
	exists, stat, err := zkdao.ZkDao.Exists(path)
	if err != nil || !exists {
		log.Errorf("Exists Error: %v, path: %s, exists: %v", err, path, exists)
		return err
	}
	data, stat, err := zkdao.ZkDao.Get(path)
	IPTopics := entity.IPTopics{}
	err = json.Unmarshal(data, &IPTopics)
	log.Infof("Reassign topic: %v", IPTopics.Topics)
	var wg sync.WaitGroup
	for _, topic := range IPTopics.Topics {
		wg.Add(1)
		go func(topic string, ipPortStr string) {
			ts.updateTopicWorkerMap(topic, ipPortStr, 0)
			wg.Done()
		}(topic, ipPortStr)
	}
	wg.Wait()
	log.Infof("Delete ip path: %s", path)
	err = zkdao.ZkDao.Delete(path, stat.Version)
	if err != nil {
		log.Errorf("RemoveTopicIP Error:%s", err)
		return err
	}
	return nil
}

func (ts *topicService) updateTopicWorkerMap(topic, ipPortStr string, workerNum int) error {

	path := util.TopicNode(topic)

	exists, _, err := zkdao.ZkDao.Exists(path)
	if err != nil || !exists {
		log.Errorf("UpdateTopicWorkerMap Exists Error path:%s", path)
		return err
	}

	ts.m.Lock()
	defer ts.m.Unlock()

	topicInfo, stat, err := zkdao.ZkDao.Get(path)
	if err != nil {
		log.Errorf("UpdateTopicWorkerMap Get Error path:%s", path)
		return err
	}

	TopicZkInfo := entity.TopicZkInfo{}
	err = json.Unmarshal(topicInfo, &TopicZkInfo)
	if ipPortStr != "" {
		key := util.GetIPPos(TopicZkInfo.Workers, ipPortStr)
		log.WithFields(log.Fields{
			"topic":     topic,
			"ipPortStr": ipPortStr,
			"key":       key,
		}).Info("Get ip pos")
		if key == -1 {
			return nil
		}
		TopicZkInfo.Workers = append(TopicZkInfo.Workers[:key], TopicZkInfo.Workers[key+1:]...)
	} else {
		TopicZkInfo.WorkerNum = workerNum
	}

	addWorkerNum := TopicZkInfo.WorkerNum - len(TopicZkInfo.Workers)

	var delWorkers []string
	if addWorkerNum < 0 {
		delWorkers = TopicZkInfo.Workers[TopicZkInfo.WorkerNum:]
		TopicZkInfo.Workers = TopicZkInfo.Workers[0:TopicZkInfo.WorkerNum]
		log.WithFields(log.Fields{
			"topic":      topic,
			"workers":    TopicZkInfo.Workers,
			"delWorkers": delWorkers,
		}).Info("Del workers")
	} else if addWorkerNum > 0 {
		var assignedWorkers []string
		assignedWorkers, err = ts.getAssignedWorkers(TopicZkInfo.WorkerNum-len(TopicZkInfo.Workers), TopicZkInfo.Workers)
		if err == nil && len(assignedWorkers) > 0 {
			TopicZkInfo.Workers = append(TopicZkInfo.Workers, assignedWorkers...)
		}
		log.WithFields(log.Fields{
			"topic":           topic,
			"assignedWorkers": assignedWorkers,
			"workers":         TopicZkInfo.Workers,
		}).Info("Reassign workers")
	}

	newZNode, err := json.Marshal(TopicZkInfo)
	if err != nil {
		log.Errorf("Marshal Error:%s", err)
		return err
	}

	b, err := zkdao.ZkDao.Set(path, newZNode, stat.Version)
	if err != nil || !b {
		log.Errorf("UpdateTopicWorkerMap Set Error:%s, b:%v", err, b)
		return err
	}
	if addWorkerNum < 0 {
		ts.updateIPTopic(REMOVENODE, delWorkers, topic)
	} else {
		ts.updateIPTopic(ADDNODE, TopicZkInfo.Workers, topic)
	}

	return nil

}

//设置worker地址缓存
func (ts *topicService) setWorkerAddressCache() ([]string, error) {
	childrens, err := ts.getAllWorkerAddress()
	if err != nil {
		return nil, err
	}
	log.Infof("Set worker address cache, %v", childrens)
	ts.workerAddressCache.Store(util.WORKER_ADDRESS_CACHE_KEY, childrens)
	return childrens, err
}

func (ts *topicService) getAllWorkerAddress() ([]string, error) {
	childrens, err := discover.DiscoverClient.DiscoverServices(util.WORKER_SERVICE)
	if err != nil {
		log.Errorf("Get worker list err, %v", err)
		return nil, err
	}
	return childrens, nil
}

//获取worker地址缓存
func (ts *topicService) getWorkerAddressCache() []string {
	workerCache, ok := ts.workerAddressCache.Load(util.WORKER_ADDRESS_CACHE_KEY)
	if ok == false {
		log.Errorf("Get worker address cache failed")
		return nil
	}
	return workerCache.([]string)
}

func (ts *topicService) GetInstanceList(serviceName string) ([]string, error) {
	childrens, err := discover.DiscoverClient.DiscoverServices(serviceName)
	if err != nil {
		log.Errorf("Get instance list err, %v", err)
		return nil, err
	}
	return childrens, nil
}

func (ts *topicService) DeleteInstance(service_name string, ip string, port int64) error {
	return discover.DiscoverClient.DeRegister(service_name, ip, port)
}

func (ts *topicService) GetIpTopics(ipPortStr string) ([]string, error) {
	path := util.IPTopicsNodeWithPortV2(ipPortStr)

	exists, _, err := zkdao.ZkDao.Exists(path)
	if err != nil || !exists {
		log.Errorf("Exists Error:%v, path:%s, exists:%v", err, path, exists)
		return nil, err
	}
	data, _, err := zkdao.ZkDao.Get(path)
	IPTopics := entity.IPTopics{}
	err = json.Unmarshal(data, &IPTopics)
	return IPTopics.Topics, nil
}
