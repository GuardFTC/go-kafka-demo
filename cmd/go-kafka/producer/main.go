// Package main @Author:冯铁城 [17615007230@163.com] 2025-09-03 10:57:01
package main

import (
	"context"
	"go-kafka-demo/common"
	go_kafka "go-kafka-demo/go-kafka"

	"github.com/sirupsen/logrus"
)

// 上下文
var c = context.Background()

func main() {

	//1.初始化日志配置
	common.InitLogConfig()

	//2.创建生产者
	producer, err := go_kafka.NewProducer(common.Addr, c)
	if err != nil {
		logrus.Errorf("create producer failed: %v", err)
		return
	}
	defer closeAndLog(producer)

	////3.发送消息不指定分区和key
	//if err := producer.SendMassage(constant.DefaultTopic, "test single msg"); err != nil {
	//	logrus.Warnf("send message error: %v", err)
	//}

	////4.发送消息指定分区 TODO 指定分区在go-kafka框架失效
	//if err := producer.SendMessageWithPartition(constant.DefaultTopic, 3, "test partition msg"); err != nil {
	//	logrus.Warnf("send message error: %v", err)
	//}

	//5.发送消息指定Key
	if err := producer.SendMessageWithKey(common.DefaultTopic, "test key", "test key msg"); err != nil {
		logrus.Warnf("send message error: %v", err)
	}

	////6.批量发送消息，不指定分区和key
	//if err := producer.SendMessages(constant.DefaultTopic, []string{"test batch msg1", "test batch msg2"}); err != nil {
	//	logrus.Warnf("send message error: %v", err)
	//}

	////7.批量发送消息，指定分区 TODO 指定分区在go-kafka框架失效
	//if err := producer.SendMessagesWithPartition(1, []string{"test batch partition msg1", "test batch partition msg2"}); err != nil {
	//	logrus.Warnf("send message error: %v", err)
	//}

	////8.批量发送消息，指定key
	//if err := producer.SendMessagesWithKey(constant.DefaultTopic, "test batch key", []string{"test batch key msg1", "test batch key msg2"}); err != nil {
	//	logrus.Warnf("send message error: %v", err)
	//}
}

// closeAndLog 关闭链接，如果异常不为空，则打印错误日志
func closeAndLog(p *go_kafka.Producer) {
	if err := p.Close(); err != nil {
		logrus.Errorf("producer close failed: %v", err)
	}
}
