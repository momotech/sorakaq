package service

type IStatisticsService interface {
	RecordElapsedTime(topicName string, msgId string, flowElapsedTime uint64, callbackElapsedTime uint64)
	QueryElapsedTime(topicName string, date string) (map[string][]interface{}, error)
}

var StatisticsService IStatisticsService
