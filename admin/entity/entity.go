package entity

type Topic struct {
	Business  string `json:"business"`
	TopicName string `json:"topic_name"`
	WorkerNum int    `json:"worker_num"`
}

type TopicZkInfo struct {
	Business   string   `json:"business"`
	TopicName  string   `json:"topic_name"`
	WorkerNum  int      `json:"worker_num"`
	Workers    []string `json:"workers"`
	CreateTime string   `json:"create_time,omitempty"`
}

type IPTopics struct {
	Topics []string `json:"topics"`
}

type Instance struct {
	ServiceName string
	IPPort      string
	Weight      int
	CreateTime  string
	UpdateTime  string
}
