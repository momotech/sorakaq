package util

import (
	"fmt"
	"strings"
)

const (
	WORKER_SERVICE           = "worker"
	CENTER_SERVICE           = "center"
	RECEIVE_SERVICE          = "receive"
	ADMIN_SERVICE            = "admin"
	ALL_SERVICE              = "all"
	WORKER_ADDRESS_CACHE_KEY = "workers"
	CENTER_ADDRESS_CACHE_KEY = "centers"
	KAFKA_TOPIC_PATH         = "/brokers/topics"
)

func RootNode() string {
	return fmt.Sprintf("/%s", "soraka")
}

// zk 临时worker节点
func TmpWorkerNode() string {
	return fmt.Sprintf("/%s/%s", "soraka", "worker")
}

// zk 临时center节点
func TmpCenterNode() string {
	return fmt.Sprintf("/%s/%s", "soraka", "center")
}

// zk 临时receive节点
func TmpReceiveNode() string {
	return fmt.Sprintf("/%s/%s", "soraka", "receive")
}

func WorkerIPPortFormat(workers []string, port int64) []string {
	var IPS []string = []string{}
	for i := 0; i < len(workers); i++ {
		IPS = append(IPS, fmt.Sprintf("%s:%d", strings.Split(workers[i], ":")[0], port))
	}
	return IPS
}

func IPTopicNode() string {
	return fmt.Sprintf("/%s/%s", "soraka", "iptopic")
}

// zk IP节点存储关联的topic
func IPTopicsNode(ip string) string {
	return fmt.Sprintf("/%s/%s/%s", "soraka", "iptopic", ip)
}

// zk IP节点存储关联的topic 带端口
func IPTopicsNodeWithPort(ip string, port int64) string {
	return fmt.Sprintf("/%s/%s/%s:%d", "soraka", "iptopic", ip, port)
}

func IPTopicsNodeWithPortV2(ipPortStr string) string {
	return fmt.Sprintf("/%s/%s/%s", "soraka", "iptopic", ipPortStr)
}

// worker启动时注册的临界时节点
func TopicWorkerNode(ip string, port int64) string {
	return fmt.Sprintf("/%s/%s/%s:%d", "soraka", "worker", ip, port)
}

func TopicWorkerNodeV2(ipPortStr string) string {
	return fmt.Sprintf("/%s/%s/%s", "soraka", "worker", ipPortStr)
}

//center启动时注册的临界时节点
func TopicCenterNode(ip string, port int64) string {
	return fmt.Sprintf("/%s/%s/%s:%d", "soraka", "center", ip, port)
}

// topic节点名字
func TopicNode(topic string) string {
	return fmt.Sprintf("/%s/%s", "soraka", topic)
}

func IPToWorkerFormat(IP string, port int64) string {
	return fmt.Sprintf("%s:%d", IP, port)
}

//lock节点
func LockNode() string {
	return fmt.Sprintf("/%s/%s", "soraka", "lock")
}

//lock临时子节点
func LockChildNode(childNodeName string) string {
	return fmt.Sprintf("/%s/%s/%s", "soraka", "lock", childNodeName)
}

func ServiceRegisterNode(serviceName string) string {
	return fmt.Sprintf("/%s/%s", "soraka", serviceName)
}

func ServiceRegisterChildNode(serviceName string, ip string, port int64) string {
	return fmt.Sprintf("/%s/%s/%s:%d", "soraka", serviceName, ip, port)
}

func ServiceRegisterChildNodeV2(serviceName string, ipPortStr string) string {
	return fmt.Sprintf("/%s/%s/%s", "soraka", serviceName, ipPortStr)
}
