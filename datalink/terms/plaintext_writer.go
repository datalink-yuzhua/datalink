package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"os"
	"time"
)

type PlaintextWriter struct {
	WriterBaseParams          // 继承了 Writer
	client           *os.File // 文件句柄
}

// NewPlaintextWriter 写入对象
func NewPlaintextWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *PlaintextWriter {
	writer := new(PlaintextWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc
	return writer
}

// 创建客户端
func (_this *PlaintextWriter) conn() (*os.File, error) {
	rc := _this.ResourceConf
	return os.OpenFile(rc.Dsn, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
}

// clientInstance 获取实例
func (_this *PlaintextWriter) clientInstance(flush bool) (*os.File, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if flush {
		var err error
		cc, err = _this.conn()
		if err != nil {
			return nil, err
		}
		_this.client = cc
	}
	return cc, nil
}

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *PlaintextWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)
	timeout := time.Second * 10
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
			_this.errC <- errors.New("taskId:" + _this.TaskId + ", timeout write")
			for _, op := range ops {
				bts, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(bts))
			}
		}
	}
	_this.exitWG.Done()
	_this.Release()
	_this.errC = nil
}

// Stop 停止
func (_this *PlaintextWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 释放资源
func (_this *PlaintextWriter) Release() {
	if _this.client != nil {
		_this.client.Close()
		_this.client = nil
	}
	_this.DocumentSetMap = nil
}

// Write 纯文本只支持写入关系
func (_this *PlaintextWriter) Write(ops []*msg.Op) (failed int) {

	cc, err := _this.clientInstance(false)
	if err != nil {
		_this.errC <- err
		return len(ops)
	}

	for _, op := range ops {
		if op.OptionType != msg.OptionTypeInsert {
			// 不支持除了insert以外的操作
			_this.errC <- errors.New("纯文本不支持除写入之外的数据操作")
			continue
		}
		bts, err := json.Marshal(op.Doc)
		if err != nil {
			_this.errC <- err
			failed++
			continue
		}
		// 默认追加一个换行符号
		bts = append(bts, '\n')
		_, err = cc.Write(bts)
		if err != nil {
			_this.errC <- err
			failed++
			continue
		}
	}
	return failed
}
