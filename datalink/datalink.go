package datalink

import (
	"data-link-2.0/datalink/linkd"
	"data-link-2.0/datalink/loop"
	"data-link-2.0/internal/conf"
	"time"
)

type DataLink struct {

	// Debug 用于输出日志
	Debug bool

	// 配置
	Conf conf.LinkConf

	// 启动动
	StartAt time.Time

	// service 容器
	Loop *loop.Loop

	// controller
	LinkD *linkd.Linkd

	// 退出
	exitC chan bool
}

func New(l *loop.Loop, d *linkd.Linkd, c conf.LinkConf) *DataLink {
	link := DataLink{Loop: l, LinkD: d, Conf: c}
	link.exitC = make(chan bool)
	return &link
}

// Start 启动 DataLink 服务
func (d *DataLink) Start() {
	d.StartAt = time.Now()
	d.Loop.Start()
	d.LinkD.Start()
}

// Wait 等待消息或者发送通知
func (d *DataLink) Wait() {
	<-d.exitC
}
