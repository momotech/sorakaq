package discover

import (
	"errors"
	"fmt"
	"time"

	"sorakaq/util"

	dao "sorakaq/dao"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type discoverClient struct {
	zkDao dao.IZkDao
}

type InstanceInfo struct {
	ServiceName string
	IPPort      string
	CreateTime  string
	UpdateTime  string
}

var (
	nodeNoExist = "/%s node is not exist"
)

func DiscoverClientInit() IDiscoveryClient {
	if DiscoverClient == nil {
		DiscoverClient = newDiscoverClient()
	}
	return DiscoverClient
}

func newDiscoverClient() IDiscoveryClient {
	return &discoverClient{
		zkDao: dao.ZkDao,
	}
}

func (d *discoverClient) Register(serviceName, instanceHost string, instancePort int64, meta string) error {
	root := util.RootNode()
	err := d.zkDao.EnsureNode(root)
	if err != nil {
		return errors.New(fmt.Sprintf(nodeNoExist, root))
	}
	serviceNode := util.ServiceRegisterNode(serviceName)
	err = d.zkDao.EnsureNode(serviceNode)
	if err != nil {
		return errors.New(fmt.Sprintf(nodeNoExist, serviceNode))
	}
	serviceChildNode := util.ServiceRegisterChildNode(serviceName, instanceHost, instancePort)
	_, err = d.zkDao.Create(serviceChildNode, []byte(meta), zk.FlagEphemeral)
	if err != nil {
		return errors.New(fmt.Sprintf("Service register err, node : %s, %v", serviceChildNode, err))
	}
	log.WithFields(log.Fields{
		"serviceName":  serviceName,
		"instanceHost": instanceHost,
		"instancePort": instancePort,
		"meta":         meta,
	}).Info("Register success")
	return nil
}

func (d *discoverClient) DeRegister(serviceName, instanceHost string, instancePort int64) error {
	serviceChildNode := util.ServiceRegisterChildNode(serviceName, instanceHost, instancePort)
	exists, stat, err := d.zkDao.Exists(serviceChildNode)
	if err != nil || !exists {
		return errors.New(fmt.Sprintf("Service deRegister err, node Exists Error:%v, path:%s, exists:%v", err, serviceChildNode, exists))
	}
	err = d.zkDao.Delete(serviceChildNode, stat.Version)
	if err != nil {
		return errors.New(fmt.Sprintf("Service deRegister err, %v", err))
	}
	log.WithFields(log.Fields{
		"serviceName":  serviceName,
		"instanceHost": instanceHost,
		"instancePort": instancePort,
	}).Info("DeRegister success")
	return nil
}

func (d *discoverClient) DiscoverServices(serviceName string) ([]string, error) {
	serviceNode := util.ServiceRegisterNode(serviceName)
	err := d.zkDao.EnsureNode(serviceNode)
	if err != nil {
		return nil, err
	}
	childrens, err := dao.ZkDao.GetChildrenNode(serviceNode)
	if err != nil {
		return nil, err
	}
	return childrens, nil
}

func (d *discoverClient) GetInstanceInfo(serviceName string, instance string) (*InstanceInfo, error) {
	serviceNode := util.ServiceRegisterChildNodeV2(serviceName, instance)
	_, stat, err := dao.ZkDao.Get(serviceNode)
	if err != nil {
		return &InstanceInfo{}, err
	}
	instanceInfo := &InstanceInfo{
		ServiceName: serviceName,
		IPPort:      instance,
		CreateTime:  time.Unix(stat.Ctime/1000, 0).Format("2006-01-02 15:04:05"),
		UpdateTime:  time.Unix(stat.Mtime/1000, 0).Format("2006-01-02 15:04:05"),
	}
	return instanceInfo, nil
}

func (d *discoverClient) Close() {
	if DiscoverClient != nil {
		DiscoverClient = nil
	}
}
