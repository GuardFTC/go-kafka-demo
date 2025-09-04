// Package main @Author:冯铁城 [17615007230@163.com] 2025-09-03 11:02:07
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
var ctx = context.Background()

func main() {

	//1.创建消费者组-1
	createConsumer(1, "group1")
	//go createConsumer(2, "group2")
}

// createConsumer 创建消费者消费消息
func createConsumer(id int, group string) {

	//1.创建消费者
	consumer := go_kafka.NewConsumer(id, ctx, addr, group, defaultTopic)
	defer consumer.Close()

	//2.监听消息
	for {
		if err := consumer.ConsumerMessage(); err != nil {
			log.Printf("%s consume fail:%s", consumer.GetTitle(), err)
		}
	}
}
