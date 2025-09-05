// Package common @Author:冯铁城 [17615007230@163.com] 2025-09-05 16:12:23
package common

import "github.com/sirupsen/logrus"

// InitLogConfig 初始化日志配置
func InitLogConfig() {

	//2.设置日志格式带颜色
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	//3.设置日志级别，默认 Info 及以上输出
	level, _ := logrus.ParseLevel("info")
	logrus.SetLevel(level)
}
