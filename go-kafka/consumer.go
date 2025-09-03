// Package go_kafka @Author:冯铁城 [17615007230@163.com] 2025-09-03 10:30:46
package go_kafka

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

// Consumer 消费者
type Consumer struct {
	r *kafka.Reader
	c context.Context
}

// NewConsumer 创建消费者
func NewConsumer(brokers []string, group string, topic string, c context.Context) *Consumer {
	return &Consumer{
		r: getConsumer(brokers, group, topic),
		c: c,
	}
}

// ConsumerMessage 消费消息
func (c *Consumer) ConsumerMessage() error {
	for {

		//1.读取消息
		m, err := c.r.FetchMessage(c.c)
		if err != nil {
			return err
		}

		//2.反序列化,获取消息
		key := string(m.Key)
		value := string(m.Value)

		//3.处理消息
		fmt.Printf("The Group Consumer:"+c.r.Config().GroupID+" Received message: key=%s, value=%s\n", key, value)

		//4.手动提交偏移量
		if err := c.r.CommitMessages(c.c, m); err != nil {
			return err
		}

		//5.如果为退出监听标识，退出循环
		if key == "exit" {
			break
		}
	}

	//6.默认返回
	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() {
	err := c.r.Close()
	if err != nil {
		log.Fatalf("failed to close consumer: %s", err)
	}
}

// getConsumer 获取消费者
func getConsumer(brokers []string, group string, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{

		//常规配置
		Brokers: brokers, //Broker地址
		Topic:   topic,   //Topic
		GroupID: group,   //消费者组

		//拉取配置
		MinBytes: 1,    // 每次拉取的最小消息大小
		MaxBytes: 10e6, // 每次拉取的最大消息大小

		//消息拉取策略
		//FirstOffset:当消费者程序启动时，从主题（Topic）分区中存在的第一条消息开始读取，然后按顺序一直消费到最新的消息，并继续等待新消息
		//LastOffset:让消费者忽略所有历史消息，只从启动之后新产生的数据开始消费
		StartOffset: kafka.FirstOffset, // 从最早的消息开始消费,

		//消费者偏移量提交配置
		//0:手动提交
		//>0:自动提交，假设当前值设置为5，消费者客户端会按照设定的时间间隔（5秒），自动将在该时间段内拉取到的所有消息的 offset 进行提交
		CommitInterval: 0, // 手动提交 offset（关闭自动提交）

		//日志配置
		Logger:      log.New(os.Stdout, "kafka-consumer-"+group+": ", log.LstdFlags),
		ErrorLogger: log.New(os.Stderr, "kafka-consumer-"+group+"-error: ", log.LstdFlags),
	})
}
