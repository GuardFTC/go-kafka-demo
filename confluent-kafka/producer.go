// Package confluent_kafka @Author:冯铁城 [17615007230@163.com] 2025-09-04 15:52:54
package confluent_kafka

import (
	"fmt"
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ProducerClient 生产者客户端
type ProducerClient struct {
	producer *kafka.Producer
}

// NewProducer 创建生产者客户端
func NewProducer(brokers []string) *ProducerClient {

	//1.创建生产者
	p, err := getProducer(brokers)
	if err != nil {
		log.Fatalf("create producer error: %v", err)
	}

	//2.创建客户端
	producerClient := &ProducerClient{
		producer: p,
	}

	//3.打印日志
	log.Printf("producer created success")

	//4.返回
	return producerClient
}

// Close 关闭生产者
func (p *ProducerClient) Close() {
	p.producer.Close()
	log.Printf("producer closed success")
}

// SendMassage 发送消息
func (p *ProducerClient) SendMassage(topic string, message string) error {
	return sendMessage(topic, -1, "", message, p.producer)
}

// SendMessageWithKey 发送消息（指定key）
func (p *ProducerClient) SendMessageWithKey(topic string, key string, message string) error {
	return sendMessage(topic, -1, key, message, p.producer)
}

// SendMessageWithPartition 发送消息（指定分区）
func (p *ProducerClient) SendMessageWithPartition(topic string, partition int32, message string) error {
	return sendMessage(topic, partition, "", message, p.producer)
}

// SendMessages 批量发送消息
func (p *ProducerClient) SendMessages(topic string, messages []string) error {
	return sendMessageBatch(topic, -1, "", messages, p.producer)
}

// SendMessagesWithKey 批量发送消息（指定key）
func (p *ProducerClient) SendMessagesWithKey(topic string, key string, messages []string) error {
	return sendMessageBatch(topic, -1, key, messages, p.producer)
}

// SendMessagesWithPartition 批量发送消息（指定分区）
func (p *ProducerClient) SendMessagesWithPartition(topic string, partition int32, messages []string) error {
	return sendMessageBatch(topic, partition, "", messages, p.producer)
}

// getProducer 创建生产者
func getProducer(brokers []string) (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{

		//基础连接配置
		"bootstrap.servers": strings.Join(brokers, ","),

		//可靠性配置
		"max.in.flight.requests.per.connection": 1,     // 限制最大并发数=1,确保消息不会乱序,但会降低吞吐量（已经发送给broker但还没收到响应确认的请求）
		"acks":                                  "all", // 等待所有副本确认（最高可靠性）
		"enable.idempotence":                    true,  // 开启幂等性 - 可能影响分区
		"retries":                               1000,  // 发送失败后重试次数
		//"retry.backoff.ms":                      100,   // 重试间隔100ms 默认值即为100ms
		//"transactional.id":                      "",    // 如需事务，设置事务ID前缀

		//超时配置
		//"request.timeout.ms":  30000,  // 请求超时30秒
		//"delivery.timeout.ms": 120000, // 整个发送流程超时2分钟
		//"socket.timeout.ms":   10000,  // socket超时10秒

		//批处理配置
		//"batch.size":         1048576, // 消息批量发送内存阈值
		//"linger.ms":          100,     // 消息批量发送时间间隔阈值
		//"batch.num.messages": 100,     // 消息批量发送数量阈值

		//压缩配置
		//"compression.type": "snappy", // Snappy压缩

		//网络配置
		//"socket.keepalive.enable": true,   // 开启链接保活
		//"connections.max.idle.ms": 300000, // 连接空闲5分钟后关闭

		//内存和缓冲配置
		//"queue.buffering.max.messages": 100000,  // 内部队列最大消息数
		//"queue.buffering.max.kbytes":   1048576, // 内部队列最大内存1GB
		//"message.max.bytes":            1000000, // 单条消息最大1MB

		//消息分区策略配置，决定消息发送到Topic的哪个分区
		//影响因素：消息顺序、负载均衡、消费者并行度
		//可选值及适用场景：
		// 1. "murmur2" (推荐):
		//    - 有Key：基于Key哈希分区，相同Key总是发送到同一分区
		//    - 无Key：轮询分区分发
		//    - 适用场景：需要保证消息顺序的业务（如用户操作日志、订单处理）
		//    - 示例：用户123的所有操作（登录->下单->支付）都在同一分区，保证顺序消费
		//
		// 2. "murmur2_random":
		//    - 有Key：基于Key哈希分区
		//    - 无Key：随机选择分区
		//    - 适用场景：部分消息需要顺序，部分消息只要负载均衡
		//
		// 3. "consistent_random":
		//    - 所有消息完全随机分布到各分区，不考虑Key
		//    - 适用场景：只追求负载均衡，不需要消息顺序（如系统监控数据）
		//
		// 4. "fnv1a" / "fnv1a_random":
		//    - 类似murmur2，但使用FNV1a哈希算法
		//    - 适用场景：需要与使用FNV1a算法的其他系统保持兼容
		//
		//选择建议：
		//- 需要消息顺序：使用"murmur2"（最常用）
		//- 只要负载均衡：使用"consistent_random"
		//- 不确定场景：使用"murmur2"（最安全的选择）
		"partitioner": "murmur2", // 基于key的哈希分区
	})
}

// sendMessage 发送单条消息
func sendMessage(topic string, partition int32, key string, message string, p *kafka.Producer) error {

	//1.创建消息发送事件通道
	deliveryChan := make(chan kafka.Event)

	//2.发送消息(将消息写入缓冲区，等待触发阈值后发送给Broker)，并传入通道
	if err := p.Produce(createMessage(topic, partition, key, message), deliveryChan); err != nil {
		return err
	}

	//3.阻塞，等待发送成功写入结果到通道
	event := <-deliveryChan

	//4.判定event类型
	switch ev := event.(type) {
	case *kafka.Message:

		//5.如果不为空，返回异常
		if ev.TopicPartition.Error != nil {
			return fmt.Errorf("send message error: [%v]", ev.TopicPartition.Error)
		}

		//6.否则打印发送结果
		log.Printf("producer send message=>[topic=%s partition=%v key=%s, value=%s] success", topic, partition, key, message)

		//7.默认返回
		return nil
	default:
		return fmt.Errorf("send message error. send result event unknow type:[%v]", ev)
	}
}

// sendBatchMessage 批量发送消息
func sendMessageBatch(topic string, partition int32, key string, messages []string, p *kafka.Producer) error {

	//1.循环封装消息
	for _, message := range messages {

		//2.创建消息
		msg := createMessage(topic, partition, key, message)

		//3.发送消息(将消息写入缓冲区，等待触发阈值后发送给Broker)
		if err := p.Produce(msg, nil); err != nil {
			return err
		}
	}

	//4.打印日志
	log.Printf("producer send message=>[topic=%s partition=%v key=%s, value=%s] success", topic, partition, key, messages)

	//5.默认返回
	return nil
}

// createMessage 创建消息
func createMessage(topic string, partition int32, key string, message string) *kafka.Message {

	//1.创建分区配置
	topicPartition := kafka.TopicPartition{Topic: &topic}

	//2.如果分区不为-1，那么设置分区
	if partition != -1 {
		topicPartition.Partition = partition
	}

	//3.创建消息
	msg := &kafka.Message{
		TopicPartition: topicPartition,
		Value:          []byte(message),
	}

	//4.如果key不为空，那么设置key
	if key != "" {
		msg.Key = []byte(key)
	}

	//5.返回消息
	return msg
}
