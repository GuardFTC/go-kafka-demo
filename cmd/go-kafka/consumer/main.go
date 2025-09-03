// Package main @Author:冯铁城 [17615007230@163.com] 2025-09-03 11:02:07
package main

import (
	"context"
	go_kafka "go-kafka-demo/go-kafka"
	"log"
	"sync"
)

// kafka服务地址
var addr = []string{"127.0.0.1:9092"}

// Kafka默认Topic
var defaultTopic = "test-go-topic"

// 上下文
var c = context.Background()

func main() {

	//1.创建waitGroup
	var wg sync.WaitGroup

	//2.设置资源总数量
	wg.Add(6)

	//3.创建第一组消费者,共4个
	go createConsumer(&wg, "group1")
	go createConsumer(&wg, "group1")
	go createConsumer(&wg, "group1")
	go createConsumer(&wg, "group1")

	//4.创建第二组消费者,共2个
	go createConsumer(&wg, "group2")
	go createConsumer(&wg, "group2")

	//5.阻塞等待资源释放
	wg.Wait()
}

// createConsumer 创建消费者消费消息
func createConsumer(wg *sync.WaitGroup, group string) {

	//1.创建消费者
	consumer := go_kafka.NewConsumer(addr, group, defaultTopic, c)
	defer consumer.Close()

	//2.监听消息
	if err := consumer.ConsumerMessage(); err != nil {
		log.Fatalf("consumer message error: %v", err)
	}

	//3.消费者协程结束，释放资源
	wg.Done()
}
