// Package confluent_kafka @Author:冯铁城 [17615007230@163.com] 2025-09-05 10:22:02
package confluent_kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

// ConsumerClient 消费者客户端
type ConsumerClient struct {
	id       int
	group    string
	ctx      context.Context
	consumer *kafka.Consumer
}

// NewConsumerClient 创建消费者客户端
func NewConsumerClient(id int, brokers []string, group string, topics []string, ctx context.Context) (*ConsumerClient, error) {

	//1.获取消费者
	consumer, err := getConsumer(brokers, group, topics)
	if err != nil {
		return nil, err
	}

	//2.创建消费者客户端
	consumerClient := &ConsumerClient{
		id:       id,
		group:    group,
		ctx:      ctx,
		consumer: consumer,
	}

	//3.打印日志
	logrus.Infof("%s create success", consumerClient.GetTitle())

	//4.返回
	return consumerClient, nil
}

// GetTitle 获取消费者标题
func (c *ConsumerClient) GetTitle() string {
	return fmt.Sprintf("consumer-%s-%v", c.group, c.id)
}

// Close 关闭消费者客户端
func (c *ConsumerClient) Close() error {
	if err := c.consumer.Close(); err != nil {
		return err
	} else {
		logrus.Infof("%s close success", c.GetTitle())
		return nil
	}
}

// StartConsume 启动消费者
func (c *ConsumerClient) StartConsume() {
	for {

		//1.监听上下文是否被取消，如果被取消则优雅退出
		select {
		case <-c.ctx.Done():
			logrus.Infof("%s is closing", c.GetTitle())
			return
		default:
		}

		//2.消费消息
		if err := c.consumeMessage(); err != nil {
			logrus.Warnf("%s consume fail:%s", c.GetTitle(), err)
		}
	}
}

// consumeMessage 消费消息
func (c *ConsumerClient) consumeMessage() error {

	//1.读取消息，超时时间100ms
	msg, err := c.consumer.ReadMessage(100 * time.Millisecond)

	//2.如果错误不为空，处理错误
	if err != nil {

		//3.处理超时错误（正常情况）
		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
			return nil
		} else {
			return err
		}
	}

	//4.处理消息
	logrus.Infof("%s receive message=>[key=%s, value=%s]", c.GetTitle(), msg.Key, msg.Value)

	//5.手动提交偏移量
	if _, err := c.consumer.CommitMessage(msg); err != nil {
		return err
	}

	//6.默认返回
	return nil
}

// getConsumer 获取消费者
func getConsumer(brokers []string, group string, topics []string) (*kafka.Consumer, error) {

	//1.创建消费者
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{

		//基础连接配置
		"bootstrap.servers": strings.Join(brokers, ","), // Kafka 集群地址
		"group.id":          group,                      // 消费组ID

		//消息拉取策略
		"auto.offset.reset":  "earliest", // 从分区的最开始的消息开始拉取
		"enable.auto.commit": false,      //开启偏移量自动提交

		//"enable.partition.eof":    true,       //读到分区末尾触发EOF事件
		//"enable.auto.commit":      true,       //开启偏移量自动提交
		//"auto.commit.interval.ms": 5000,       //偏移量自动提交频率
	})

	//2.err不为空直接返回
	if err != nil {
		return nil, err
	}

	//3.订阅topic
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	//4.返回消费者
	return consumer, nil
}
