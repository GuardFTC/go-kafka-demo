// Package go_kafka @Author:冯铁城 [17615007230@163.com] 2025-09-02 15:31:21
package go_kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

// KafkaClient Kafka客户端
type KafkaClient struct {
	w *kafka.Writer
	r *kafka.Reader
	c context.Context
}

// NewKafkaClient 创建Kafka客户端
func NewKafkaClient(brokers []string, topic string) *KafkaClient {
	return &KafkaClient{
		w: getWriter(brokers, topic),
		r: nil,
	}
}

// CloseKafkaClient 关闭Kafka客户端
func (k *KafkaClient) CloseKafkaClient() {

	//1.关闭生产者连接
	err := k.w.Close()
	if err != nil {
		log.Fatalf("failed to close writer: %s", err)
	}

	//2.关闭消费者连接
	//err = k.r.Close()
	//if err != nil {
	//	log.Fatalf("failed to close reader: %s", err)
	//}
}

// GetWriter 获取生产者
func (k *KafkaClient) GetWriter() *kafka.Writer {
	return k.w
}

// GetReader 获取消费者
func (k *KafkaClient) GetReader() *kafka.Reader {
	return k.r
}

// WithContext 设置上下文
func (k *KafkaClient) WithContext(c context.Context) *KafkaClient {
	k.c = c
	return k
}

// SendMassage 发送消息
func (k *KafkaClient) SendMassage(topic string, message string) error {
	return sendMessage(topic, -1, "", message, k.w, k.c)
}

// SendMessageWithKey 发送消息（指定key）
func (k *KafkaClient) SendMessageWithKey(topic string, key string, message string) error {
	return sendMessage(topic, -1, key, message, k.w, k.c)
}

// SendMessageWithPartition 发送消息（指定分区）
func (k *KafkaClient) SendMessageWithPartition(topic string, partition int, message string) error {
	return sendMessage(topic, partition, "", message, k.w, k.c)
}

// SendMessages 批量发送消息
func (k *KafkaClient) SendMessages(topic string, messages []string) error {
	return sendMessageBatch(topic, -1, "", messages, k.w, k.c)
}

// SendMessagesWithKey 批量发送消息（指定key）
func (k *KafkaClient) SendMessagesWithKey(topic string, key string, messages []string) error {
	return sendMessageBatch(topic, -1, key, messages, k.w, k.c)
}

// SendMessagesWithPartition 批量发送消息（指定分区）
func (k *KafkaClient) SendMessagesWithPartition(topic string, partition int, messages []string) error {
	return sendMessageBatch(topic, partition, "", messages, k.w, k.c)
}
