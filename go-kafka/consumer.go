// Package go_kafka @Author:冯铁城 [17615007230@163.com] 2025-09-03 10:30:46
package go_kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// Consumer 消费者
type Consumer struct {
	id     int
	ctx    context.Context
	reader *kafka.Reader
}

// NewConsumer 创建消费者
func NewConsumer(id int, context context.Context, brokers []string, group string, topic string) *Consumer {

	//1.创建消费者
	consumer := &Consumer{
		id:     id,
		ctx:    context,
		reader: getConsumer(strconv.Itoa(id), brokers, group, topic),
	}

	//2.打印日志
	log.Printf("%s created success", consumer.GetTitle())

	//3.返回
	return consumer
}

// GetTitle 获取消费者标题
func (c *Consumer) GetTitle() string {
	return fmt.Sprintf("consumer-%s-%v", c.reader.Config().GroupID, c.id)
}

// Close 关闭消费者
func (c *Consumer) Close() {
	err := c.reader.Close()
	if err != nil {
		log.Fatalf("%s closed fail:%s", c.GetTitle(), err)
	} else {
		log.Printf("%s closed success", c.GetTitle())
	}
}

// StartConsume 启动消费者
func (c *Consumer) StartConsume() {
	for {

		//1.监听上下文是否被取消，如果被取消则优雅退出
		select {
		case <-c.ctx.Done():
			log.Printf("%s is closing", c.GetTitle())
			return
		default:
		}

		//2.未取消则继续消费
		if err := c.consumeMessage(); err != nil {

			//3.检测是否是因为上下文取消导致的异常，如果是，则优雅退出
			if c.ctx.Err() != nil {
				log.Printf("%s is closing", c.GetTitle())
				return
			}

			//4.如果不是主动退出导致的异常，则打印错误信息
			log.Printf("%s consume fail:%s", c.GetTitle(), err)
		}
	}
}

// consumeMessage 消费消息
func (c *Consumer) consumeMessage() error {

	//1.读取消息
	m, err := c.reader.FetchMessage(c.ctx)
	if err != nil {
		return err
	}

	//2.反序列化,获取消息
	key := string(m.Key)
	value := string(m.Value)

	//3.处理消息
	log.Printf("%s receive message=>[key=%s, value=%s]", c.GetTitle(), key, value)

	//4.手动提交偏移量
	if err := c.reader.CommitMessages(c.ctx, m); err != nil {
		return err
	}

	//5.默认返回
	return nil
}

// getConsumer 获取消费者
func getConsumer(id string, brokers []string, group string, topic string) *kafka.Reader {
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
		//Logger: log.New(os.Stdout, "consumer-"+group+"-"+id+": ", log.LstdFlags),
		ErrorLogger: log.New(os.Stderr, "consumer-"+group+"-"+id+" error: ", log.LstdFlags),
	})
}
