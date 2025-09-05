// Package main @Author:冯铁城 [17615007230@163.com] 2025-09-03 11:02:07
package main

import (
	"context"
	"go-kafka-demo/constant"
	go_kafka "go-kafka-demo/go-kafka"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// 类似于CountDownLatch 用于监听一组goroutine
var wg sync.WaitGroup
var goroutineNum = 0

func main() {

	//1.添加信号监听
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	//2.创建带取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//3.创建消费者组-1
	createConsumer(1, "group1", ctx)
	createConsumer(2, "group1", ctx)
	createConsumer(3, "group1", ctx)

	//4.设置监听数量
	wg.Add(goroutineNum)

	//5.等待关闭信号
	<-c
	log.Println("receive close sign. try to closing consumers")

	//6.收到关闭信号后，取消context，通知goroutine停止
	cancel()

	//7.等待goroutine结束
	wg.Wait()
}

// createConsumer 创建消费者消费消息
func createConsumer(id int, group string, ctx context.Context) {

	//1.消费者数量++
	goroutineNum++

	//2.创建协程，执行消费逻辑
	go func() {

		//3.确保最终可以释放锁资源
		defer wg.Done()

		//4.创建消费者
		consumer := go_kafka.NewConsumer(id, ctx, constant.Addr, group, constant.DefaultTopic)
		defer consumer.Close()

		//5.在goroutine中处理消费
		for {

			//6.监听上下文是否被取消，如果被取消则优雅退出
			select {
			case <-ctx.Done():
				log.Printf("%s is closing", consumer.GetTitle())
				return
			default:
			}

			//7.未取消则继续消费
			if err := consumer.ConsumerMessage(); err != nil {

				//8.检测是否是因为上下文取消导致的异常，如果是，则优雅退出
				if ctx.Err() != nil {
					log.Printf("%s is closing", consumer.GetTitle())
					return
				}

				//9.如果不是主动退出导致的异常，则打印错误信息
				log.Printf("%s consume fail:%s", consumer.GetTitle(), err)
			}
		}
	}()
}
