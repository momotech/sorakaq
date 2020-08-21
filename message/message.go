package message

const (
	NORMAO_DELAY_MSG = 0
	CANCEL_MSG       = 1
	ADD_TIME_MSG     = 2
)

type ReqParams struct {
	TopicName   string   `json:"topic_name"`
	DelayTime   uint64   `json:"delay_time"`
	Ttr         int      `json:"ttr"` //Callback timeout time ms
	Callback    Callback `json:"callback"`
	MsgId       string   `json:"msg_id"`
	AddTime     uint64   `json:"add_time"`
	ServiceName string   `json:"service_name"`
	Instance    string   `json:"instance"`
	StartTime   uint64   `json:"start_time"`
	MsgNum      int      `json:"num"`
	Date        string   `json:"date"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
}

type Callback struct {
	RequestType string `json:"request_type"`
	Url         string `json:"url"`
	Params      string `json:"params"`
}

type DelayMsg struct {
	TopicName   string  `json:"topic_name"`
	MsgId       string  `json:"msg_id"`
	MsgType     uint8   `json:"msg_type"`
	DelayTime   uint64  `json:"delay_time"`
	Ttr         int     `json:"ttr"` //Callback timeout time ms
	WorkerId    string  `json:"worker_id"`
	WorkerIp    string  `json:"worker_ip,omitempty"`
	SendTime    uint64  `json:"time_stamp"`
	RequestType string  `json:"request_type"`
	Body        MsgBody `json:"body"`
}

type MsgBody struct {
	URL    string `json:"url"`
	Params string `json:"params"`
}

type Request struct {
	Msg DelayMsg `json:"msg"`
}

type Response struct {
	Ec     int    `json:"ec"`
	Em     string `json:"em"`
	Result bool   `json:"result"`
}
