package util

import (
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

const (
	SUCCESS                       = 200
	SYSTEM_ERROR                  = 500
	INVALID_PARAMS                = 401
	INVALID_TOPIC_NAME            = 402
	INVALID_SERVICE_NAME          = 403
	MSG_NUM_LIMIT                 = 404
	MSG_NOT_EXIST                 = 405
	INCORRECT_ACCOUNT_OR_PASSWORD = 406
	DELAY_TIME_EXCEEDS_THE_LIMIT  = 501

	SEVEN_DAY = 7 * 24 * 3600 * 1000
)

var RespMsg = map[int]string{
	200: "success",
	500: "system error",
	401: "invalid params",
	402: "topic name cannot be set [admin/center/worker/receive/lock/iptopic]",
	403: "service name can be only [admin/center/worker/receive/all]",
	404: "maximum num of messages is limit 500",
	405: "msg is not exist",
	406: "login failed, incorrect account or password",
	501: "the longest delay time is 7 days",
}

type Response struct {
	Ec     int         `json:"ec"`
	Em     string      `json:"em"`
	Result interface{} `json:"result"`
}

func RespJson(errno int, result interface{}) *Response {
	var response = new(Response)
	response.Ec = errno
	response.Em = RespMsg[errno]
	response.Result = result
	return response
}

func ParseIntParam(form url.Values, keys ...string) int {
	for _, key := range keys {
		v := form.Get(key)
		if v != "" {
			value, _ := strconv.Atoi(v)
			return value
		}
	}
	return 0
}

func ParseStringParam(form url.Values, keys ...string) string {
	for _, key := range keys {
		v := form.Get(key)
		if v != "" {
			return v
		}
	}
	return ""
}

func Wait() os.Signal {
	sigCh := make(chan os.Signal, 2)

	signal.Notify(
		sigCh,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM,
		syscall.SIGUSR2,
		syscall.SIGHUP,
	)

	for {
		sig := <-sigCh
		switch sig {
		case syscall.SIGINT:
			return sig
		case syscall.SIGQUIT:
			return sig
		case syscall.SIGTERM:
			return sig
		case syscall.SIGUSR2:
			continue
		case syscall.SIGHUP:
			continue
		}
	}
}
