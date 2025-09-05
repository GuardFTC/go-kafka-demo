// package main @Author:冯铁城 [17615007230@163.com] 2025-09-05 11:21:27
package main

import (
	"context"
	"go-kafka-demo/common"
	confluent_kafka "go-kafka-demo/confluent-kafka"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

// 类似于CountDownLatch 用于监听一组goroutine
var wg sync.WaitGroup

func main() {

	//1.初始化日志配置
	common.InitLogConfig()

	//2.添加信号监听
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	//3.创建带取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//4.创建消费者组-1
	createConsumer(1, "group1", ctx)
	createConsumer(2, "group1", ctx)
	createConsumer(3, "group1", ctx)
	createConsumer(4, "group1", ctx)

	//5.等待关闭信号
	<-c
	logrus.Info("receive close sign. try to closing consumers")

	//6.收到关闭信号后，取消context，通知goroutine停止
	cancel()

	//7.等待所有消费者关闭
	wg.Wait()

	//8.打印最终日志
	logrus.Info("all consumers closed")
}

// createConsumer 创建消费者
func createConsumer(id int, group string, ctx context.Context) {

	//1.计数器+1
	wg.Add(1)

	//2.创建协程执行消费逻辑
	go func() {

		//3.确保最终可以释放锁资源
		defer wg.Done()

		//4.创建消费者
		consumer, err := confluent_kafka.NewConsumerClient(id, common.Addr, group, []string{common.DefaultTopic}, ctx)
		if err != nil {
			logrus.Errorf("consumer-%s-%v create failed: %v", group, id, err)
			return
		}
		defer closeAndLog(consumer)

		//5,启动消费者
		consumer.StartConsume()
	}()
}

// closeAndLog 关闭链接，如果异常不为空，则打印错误日志
func closeAndLog(c *confluent_kafka.ConsumerClient) {
	if err := c.Close(); err != nil {
		logrus.Errorf("consumer [%s] close failed: %v", c.GetTitle(), err)
	}
}
