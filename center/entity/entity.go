package entity

import "github.com/Shopify/sarama"

type TopicZkInfo struct {
	Business  string   `json:"business"`
	TopicName string   `json:"topicName"`
	Workers   []string `json:"workers"`
}

type KafkaConfig struct {
	BrokerServers string
	Config        *sarama.Config
	GroupID       string
	TopicPrefix   string
}
