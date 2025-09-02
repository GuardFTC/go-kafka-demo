// Package go_kafka @Author:冯铁城 [17615007230@163.com] 2025-09-02 14:25:09
package go_kafka

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// SendSingleMessage 发送消息
func getWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{

		//常规配置
		Addr:  kafka.TCP(brokers...), //Broker地址
		Async: false,                 //是否异步发送，追求强一致性时候设置为false

		//发送Ack确认
		RequiredAcks: kafka.RequireAll, //消息发送到分区的Leader副本，以及大多数follow副本后，才返回发布成功

		//发送失败重试次数
		MaxAttempts: 5, // 包含首次尝试 4+1

		//可靠性配置
		WriteTimeout: 30 * time.Second, // 整个发送流程的总时间限制 (连接建立 + 发送数据 + 等待响应 + 重试)
		ReadTimeout:  10 * time.Second, // 等待响应的时间限制 要求小于WriteTimeout

		//消息批处理阈值配置
		BatchSize:    100,                    // 数量阈值，当缓冲区消息=该数量时，将消息批量发送到Broker
		BatchTimeout: 100 * time.Millisecond, // 时间阈值，当距离上一次发送时间超过该时间间隔，将消息批量发送到Broker
		BatchBytes:   1048576,                // 内存阈值，当缓冲区消息大小超过该阈值，将消息批量发送到Broker

		//压缩配置
		Compression: kafka.Snappy, // Snappy压缩，平衡压缩率和性能

		//网络配置
		Transport: &kafka.Transport{
			DialTimeout: 5 * time.Second,   //最大建立连接时间，超过该时间仍然未与Broker建立连接，则认为连接已断开
			IdleTimeout: 300 * time.Second, //最大连接空闲时间，超过该时间则断开连接
		},

		//日志配置
		Logger:      log.New(os.Stdout, "kafka-producer: ", log.LstdFlags),
		ErrorLogger: log.New(os.Stderr, "kafka-error: ", log.LstdFlags),

		//消息分发路由策略，包括以下5种
		// 1. LeastBytes:
		//    基于“目的分区当前积压的待发送字节数”选择分区——谁最“空”就发给谁。
		//    适合追求吞吐与均衡（热点更少），但同一 Key 可能落到不同分区，无法保证按 Key 的有序性。
		//    示例：Balancer: &kafka.LeastBytes{}

		// 2. RoundRobin:
		//    轮询分区：按 0,1,2,... 依次发送，简单可预期；不同实例间不共享状态。
		//    适合无序要求的均匀分布场景；同一 Key 不保证固定分区，无法按 Key 保序。
		//    示例：Balancer: &kafka.RoundRobin{}

		// 3. Hash:
		//    根据消息的 Key 进行哈希路由：同一 Key -> 固定分区（分区数不变时）。
		//    适合“按用户/订单”等实体做顺序消费与聚合处理的场景；需确保每条消息都有稳定的 Key。
		//    示例：Balancer: &kafka.Hash{}

		// 4. CRC32Balancer:
		//    也是“按 Key 哈希”到分区，但使用 CRC32 作为哈希函数的实现版本。
		//    行为与 Hash 类似：同 Key 固定分区（在分区数稳定时），用于需要与 CRC32 策略对齐的情况。
		//    示例：Balancer: &kafka.CRC32Balancer{}

		// 5. ConsistentHash:
		//    一致性哈希，将分区映射到哈希环，分区增删时 Key 的迁移量更小，路由更稳定。
		//    适合可能会调整分区数量、但又希望尽量保持 Key->分区稳定映射的场景；同样需要稳定 Key。
		//    示例：Balancer: &kafka.ConsistentHash{}
		Balancer: &kafka.Hash{},
	}
}

// sendMessage 发送单条消息
func sendMessage(topic string, partition int, key string, message string, w *kafka.Writer, c context.Context) error {

	//1.校验
	if c == nil {
		return errors.New("context can not be nil")
	}

	//2.创建消息
	msg := getMessage(topic, partition, key, message)

	//3.发送消息
	if err := w.WriteMessages(c, msg); err != nil {
		return err
	} else {
		return nil
	}
}

// SendMessageBatch 批量发送消息
func sendMessageBatch(topic string, partition int, key string, messages []string, w *kafka.Writer, c context.Context) error {

	//1.校验
	if c == nil {
		return errors.New("context can not be nil")
	}

	//2.创建消息切片
	var msgs []kafka.Message

	//3.循环封装消息
	for _, message := range messages {

		//4.创建消息
		msg := getMessage(topic, partition, key, message)

		//5.写入切片
		msgs = append(msgs, msg)
	}

	//6.批量发送消息
	if err := w.WriteMessages(c, msgs...); err != nil {
		return err
	} else {
		return nil
	}
}

// getMessage 创建消息
func getMessage(topic string, partition int, key string, message string) kafka.Message {

	//1.创建消息
	msg := kafka.Message{
		Topic: topic,
		Value: []byte(message),
	}

	//2.如果key不为空，写入key
	if key != "" {
		msg.Key = []byte(key)
	}

	//3.如果分区不为空，指定分区
	if partition != -1 {
		msg.Partition = partition
	}

	//4.返回消息
	return msg
}
