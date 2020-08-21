package service

import "sorakaq/message"

type IReceiveService interface {
	produceMsg(topic string, delayMsg []byte) error
	Stop()
	Submit(req *message.ReqParams) (string, error)
	Cancel(req *message.ReqParams) bool
	AddTime(req *message.ReqParams) bool
	process(req interface{}, msgType int) bool
}

var ReceiveService IReceiveService
