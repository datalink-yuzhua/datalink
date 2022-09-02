package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"time"
)

type EmptyWriter struct {
	WriterBaseParams // 继承了 Writer
}

// NewEmptyWriter es写入
func NewEmptyWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *EmptyWriter {
	writer := new(EmptyWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.ResourceConf = rc
	return writer
}

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *EmptyWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)
	timeout := time.Second * 60
	for !_this.exitF {
		ops := <-ch

		// 结束命令
		if len(ops) == 1 && ops[0].MessageType == msg.MessageTypeCmd {
			op := ops[0]
			switch op.Cmd.Type {
			case msg.CmdTypeReadDone:
				v, ok := op.Cmd.Value.(bool)
				if ok && v == true {
					_this.exitF = true
				}
			}
			continue
		}

		// 设置写入超时
		failed := 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		go func(ctx context.Context) {
			failed = _this.Write(ops)
			cancel()
		}(ctx)
		select {
		case <-ctx.Done():
			call(failed)
		case <-time.After(timeout):
			call(len(ops))
			cancel()
			_this.errC <- errors.New("任务:" + _this.TaskId + ", 写入超时")
			for _, op := range ops {
				msg, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(msg))
			}
		}
	}
	_this.exitWG.Done()
	_this.Release()
	_this.errC = nil
}

// Stop 停止
func (_this *EmptyWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 释放资源
func (_this *EmptyWriter) Release() {}

// Write 设置批量刷写
func (_this *EmptyWriter) Write(ops []*msg.Op) (failed int) {
	return 0
}
