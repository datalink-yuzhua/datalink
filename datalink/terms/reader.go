package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/msg"
	"errors"
	"sync"
)

var EmptyError = errors.New("")

// ReaderBaseParams reader公共参数
type ReaderBaseParams struct {
	Id           int            // id
	TaskId       string         // taskId
	ResourceConf conf.Resource  // 资源配置
	SourceConf   conf.Reader    // 事件源,读取
	ResumeFile   string         // resume文件位置
	errC         chan error     // task error
	exitF        bool           // 退出标志位
	exitWG       sync.WaitGroup // wg
	lock         sync.RWMutex   // 锁
	err          error          // 任务错误
}

type ReaderRunImp interface {

	// Run 运行任务
	Run(opC chan *msg.Op, errC chan error)
	// Stop 停止,手动控制
	Stop()
	// Err 错误
	Err() error
	// Clear 清理资源
	Clear()

	// SyncMode 同步模式
	SyncMode() string
	// RelateOneByOne 一对一关联条件
	RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{})
	// RelateOneByMany 一对多关联关系
	RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{})
}

type ReaderTermImp interface {
	// Direct 常规方式查表导数据
	Direct(opC chan *msg.Op)
	// Stream 流读取
	Stream(opC chan *msg.Op)
	// Replica 副本模式
	Replica(opC chan *msg.Op)
	// Release 释放资源
	Release()
}
