// Package main @Author:冯铁城 [17615007230@163.com] 2025-09-04 15:34:30
package main

import (
	confluent_kafka "go-kafka-demo/confluent-kafka"
	"log"
)

// kafka服务地址
var addr = []string{"127.0.0.1:9092"}

// Kafka默认Topic
var defaultTopic = "test-go-topic"

func main() {

	//1.创建生产者
	producer := confluent_kafka.NewProducer(addr)
	defer producer.Close()

	////2.发送消息不指定分区和key
	//if err := producer.SendMassage(defaultTopic, "test single msg"); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	////2.发送消息指定分区
	//if err := producer.SendMessageWithPartition(defaultTopic, 3, "test partition msg"); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	////3.发送消息指定Key
	//if err := producer.SendMessageWithKey(defaultTopic, "test key", "test key msg"); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	////4.批量发送消息，不指定分区和key
	//if err := producer.SendMessages(defaultTopic, []string{"test batch msg1", "test batch msg2"}); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	////5.批量发送消息，指定分区
	//if err := producer.SendMessagesWithPartition(defaultTopic, 3, []string{"test batch partition msg1", "test batch partition msg2"}); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	//6.批量发送消息，指定key
	if err := producer.SendMessagesWithKey(defaultTopic, "order123", []string{"test batch key msg1", "test batch key msg2"}); err != nil {
		log.Fatalf("send message error: %v", err)
	}
}
