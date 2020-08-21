package discover

type IDiscoveryClient interface {
	//服务注册
	Register(serviceName, instanceHost string, instancePort int64, meta string) error

	//服务注销
	DeRegister(serviceName, instanceHost string, instancePort int64) error

	//发现服务实例
	DiscoverServices(serviceName string) ([]string, error)

	//获取实例信息
	GetInstanceInfo(serviceName string, instance string) (*InstanceInfo, error)

	//关闭服务注册客户端
	Close()
}

var DiscoverClient IDiscoveryClient
