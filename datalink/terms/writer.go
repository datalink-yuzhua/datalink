package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/msg"
	"sync"
)

// WriterBaseParams 目标对象,用于写入对象
type WriterBaseParams struct {
	Id             int               // id
	TaskId         string            // taskId
	ResourceConf   conf.Resource     // 资源
	TargetConf     conf.Writer       // 目标项目
	DocumentSetMap map[string]string // 数据集映射
	errC           chan error        // task error
	exitF          bool              // 退出标记
	exitWG         sync.WaitGroup    // 退出WG
}

type WriterTermImp interface {
	// Run 运行任务
	Run(chan []*msg.Op, chan error, func(int))
	// Write 写入数据
	Write([]*msg.Op) (failed int)
	// Stop 手动控制退出
	Stop()
	// Release 释放资源
	Release()
}
