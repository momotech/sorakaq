package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	recovery "sorakaq/recovery/service"

	"sorakaq/message"

	"github.com/gofrs/uuid"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	//	_ "net/http/pprof"
)

type receiveService struct {
	kafkaProducer sarama.SyncProducer
}

func NewReceiveService(brokerList string) (IReceiveService, error) {
	Config := sarama.NewConfig()
	Config.Producer.RequiredAcks = sarama.WaitForAll
	Config.Producer.Partitioner = sarama.NewRandomPartitioner
	Config.Producer.Return.Successes = true
	Config.Producer.Return.Errors = true

	Client, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), Config)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("New sync producer err: %v", err))
	}
	return &receiveService{
		kafkaProducer: Client,
	}, nil
}

func (this *receiveService) produceMsg(topic string, delayMsg []byte) error {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(delayMsg)
	partition, offset, err := this.kafkaProducer.SendMessage(msg)
	if err != nil {
		log.WithFields(log.Fields{
			"topic":     topic,
			"delayMsg":  string(delayMsg),
			"partition": partition,
			"offset":    offset,
		}).Error("produce msg err")
		return err
	}
	log.WithFields(log.Fields{
		"topic":     topic,
		"delayMsg":  string(delayMsg),
		"partition": partition,
		"offset":    offset,
	}).Debug("After Produce msg")
	return nil
}

func (this *receiveService) Submit(req *message.ReqParams) (string, error) {
	msgId := uuid.Must(uuid.NewV4())
	ttr := req.Ttr
	if ttr == 0 {
		ttr = 1000
	}
	delayMsg := &message.DelayMsg{
		TopicName:   req.TopicName,
		MsgId:       msgId.String(),
		MsgType:     message.NORMAO_DELAY_MSG,
		DelayTime:   req.DelayTime,
		Ttr:         ttr,
		SendTime:    uint64(time.Now().UnixNano() / 1e6),
		RequestType: req.Callback.RequestType,
		Body:        message.MsgBody{URL: req.Callback.Url, Params: req.Callback.Params},
	}
	msg, err := json.Marshal(delayMsg)
	if err != nil {
		log.Errorf("Marshal msg err, %v, %v", err, delayMsg)
		return "", err
	}
	log.Debugf("Before produce msg: %v", delayMsg)
	return msgId.String(), this.produceMsg(req.TopicName, msg)
}

func (this *receiveService) Cancel(req *message.ReqParams) bool {
	return this.process(req, message.CANCEL_MSG)
}

func (this *receiveService) AddTime(req *message.ReqParams) bool {
	return this.process(req, message.ADD_TIME_MSG)
}

func (this *receiveService) process(req interface{}, msgType int) bool {
	var msgId string
	var addTime uint64
	if msgType == message.CANCEL_MSG {
		msgReq := req.(*message.ReqParams)
		msgId = msgReq.MsgId
	} else {
		msgReq := req.(*message.ReqParams)
		msgId = msgReq.MsgId
		addTime = msgReq.AddTime
	}
	workerId := recovery.RecoveryService.GetWorkerIdByMsgId(msgId)
	if len(workerId) == 0 {
		return false
	}
	msg, err := recovery.RecoveryService.GetWorkerMsgByMsgId(workerId, msgId)
	if err != nil {
		log.Errorf("Get worker msg err, msg:%s, err:%v", msg, err)
		return false
	}
	delayMsg := new(message.DelayMsg)
	err = json.Unmarshal([]byte(msg), &delayMsg)
	if err != nil {
		log.Errorf("Unmarshal msg err, msg:%s, err:%v", msg, err)
		return false
	}
	if msgType == message.CANCEL_MSG {
		delayMsg.MsgType = message.CANCEL_MSG
	} else {
		delayMsg.MsgType = message.ADD_TIME_MSG
		delayMsg.DelayTime = addTime
	}
	var msgDetail []byte
	msgDetail, err = json.Marshal(delayMsg)
	if err != nil {
		log.Errorf("Marshal msg err, %v, %v", err, delayMsg)
		return false
	}
	return recovery.RecoveryService.UpdateWorkerMsg(workerId, delayMsg.MsgId, msgDetail)
}

func (this *receiveService) Stop() {
	if this.kafkaProducer != nil {
		this.kafkaProducer.Close()
	}
}
