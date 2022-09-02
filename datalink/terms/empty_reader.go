package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/msg"
	"fmt"
	"time"
)

type EmptyReader struct {
	ReaderBaseParams // 继承了 Reader
}

// NewEmptyReader 构建空对象客户端
func NewEmptyReader(id int, tid string, rc conf.Resource, sc conf.Reader) *EmptyReader {
	reader := new(EmptyReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	return reader
}

// Run 运行任务
func (_this *EmptyReader) Run(opC chan *msg.Op, errC chan error) {
	_this.errC = errC
	_this.exitWG.Add(1)
	go func() {
		defer func() {
			_this.exitWG.Done()
		}()

		switch _this.SourceConf.SyncMode {
		case cst.SyncModeDirect:
			_this.Direct(opC)
		case cst.SyncModeStream:
			_this.Stream(opC)
		case cst.SyncModeReplica:
			_this.Replica(opC)
		case cst.SyncModeEmpty:
			// pass不需要释放对象,需要处理
		}
		_this.exitF = true
	}()

	// 当为stream时需要监控退出
	switch _this.SourceConf.SyncMode {
	case cst.SyncModeReplica, cst.SyncModeStream:
		_this.exitWG.Add(1)
		go func() {
			defer func() {
				_this.exitWG.Done()
			}()

			tk := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-tk.C:
					if !_this.exitF {
						continue
					}
					return
				}
			}
		}()
	}

	// 等待读取结束
	_this.exitWG.Wait()
}

// Stop 停止任务
func (_this *EmptyReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *EmptyReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *EmptyReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *EmptyReader) Release() {
}

// SyncMode 同步模式
func (_this *EmptyReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// Direct 读取数据
func (_this *EmptyReader) Direct(opC chan *msg.Op) {
	// 百万数据演示
	for i := 1; i < 1000001; i++ {
		if _this.exitF {
			return
		}

		docSet := "empty"
		docId := fmt.Sprint(i)
		doc := map[string]interface{}{
			"field1": i,
			"field2": i,
			"field3": i,
			"field4": i,
			"field5": i,
			"field6": i,
			"field7": i,
			"field8": i,
			"field9": i,
			"field0": i,
		}
		op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
		op.SourceId = _this.Id
		op.DocumentSet = docSet
		op.DocumentSetAlias = docSet
		op.OptionType = msg.OptionTypeInsert
		opC <- op
	}
}

// Stream 流读取
func (_this *EmptyReader) Stream(opC chan *msg.Op) {
	i := 1
	for {
		if _this.exitF {
			return
		}

		docSet := "empty"
		docId := fmt.Sprint(i)
		doc := map[string]interface{}{
			"field1": i,
			"field2": i,
			"field3": i,
			"field4": i,
			"field5": i,
			"field6": i,
			"field7": i,
			"field8": i,
			"field9": i,
			"field0": i,
		}
		op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
		op.SourceId = _this.Id
		op.DocumentSet = docSet
		op.DocumentSetAlias = docSet
		op.OptionType = msg.OptionTypeInsert
		opC <- op

		i++
	}
}

// Replica 副本模式
func (_this *EmptyReader) Replica(opC chan *msg.Op) {
	_this.Stream(opC)
}

// RelateOneByOne 一对一关联条件
func (_this *EmptyReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *EmptyReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}
