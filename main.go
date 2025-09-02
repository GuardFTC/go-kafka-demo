// package main @Author:冯铁城 [17615007230@163.com] 2025-09-02 14:22:08
package main

import (
	"context"
	go_kafka "go-kafka-demo/go-kafka"
	"log"
)

// kafka服务地址
var addr = []string{"127.0.0.1:9092"}

// Kafka默认Topic
var defaultTopic = "test-go-topic"

// 上下文
var c = context.Background()

func main() {

	//1.获取go-kafka客户端
	client := go_kafka.NewKafkaClient(addr, defaultTopic)
	defer client.CloseKafkaClient()

	//2.发送消息不指定分区和key
	if err := client.WithContext(c).SendMassage(defaultTopic, "test single msg"); err != nil {
		log.Fatalf("send message error: %v", err)
	}

	////3.发送消息指定分区 TODO 指定分区在go-kafka框架失效
	//if err := client.WithContext(c).SendMessageWithPartition(defaultTopic, 3, "test partition msg"); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	//4.发送消息指定Key
	if err := client.WithContext(c).SendMessageWithKey(defaultTopic, "test key", "test key msg"); err != nil {
		log.Fatalf("send message error: %v", err)
	}

	//5.批量发送消息，不指定分区和key
	if err := client.WithContext(c).SendMessages(defaultTopic, []string{"test batch msg1", "test batch msg2"}); err != nil {
		log.Fatalf("send message error: %v", err)
	}

	////6.批量发送消息，指定分区 TODO 指定分区在go-kafka框架失效
	//if err := client.WithContext(c).SendMessagesWithPartition(1, []string{"test batch partition msg1", "test batch partition msg2"}); err != nil {
	//	log.Fatalf("send message error: %v", err)
	//}

	//7.批量发送消息，指定key
	if err := client.WithContext(c).SendMessagesWithKey(defaultTopic, "test batch key", []string{"test batch key msg1", "test batch key msg2"}); err != nil {
		log.Fatalf("send message error: %v", err)
	}
}
