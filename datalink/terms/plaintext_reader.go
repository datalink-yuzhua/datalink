package terms

import (
	"bufio"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"io"
	"os"
	"time"
)

// PlaintextReader 存文本读取
type PlaintextReader struct {
	ReaderBaseParams          // Reader
	client           *os.File // 文件句柄
}

// NewPlainTextReader 打开文件
func NewPlainTextReader(id int, tid string, rc conf.Resource, sc conf.Reader) *PlaintextReader {
	reader := new(PlaintextReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	return reader
}

// clientInstance 获取实例
func (_this *PlaintextReader) clientInstance(flush bool) (*os.File, error) {
	cc := _this.client
	if cc != nil {
		flush = true
	}
	if flush {
		var err error
		cc, err = PlainTextConn(_this.ResourceConf)
		if err != nil {
			return nil, err
		}
		_this.client = cc
	}
	return cc, nil
}

// Run 运行任务
func (_this *PlaintextReader) Run(opC chan *msg.Op, errC chan error) {
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
func (_this *PlaintextReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *PlaintextReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *PlaintextReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *PlaintextReader) Release() {
	log.Info("plaintext source stop")
	if _this.client != nil {
		_this.client.Close()
		_this.client = nil
	}
}

// SyncMode 同步模式
func (_this *PlaintextReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
func (_this *PlaintextReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *PlaintextReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// Direct 读取数据
func (_this *PlaintextReader) Direct(opC chan *msg.Op) {
	instance, err := _this.clientInstance(true)
	if err != nil {
		_this.err = err
		return
	}

	br := bufio.NewReader(instance)
	for docSet, docSetup := range _this.SourceConf.DocumentSet {
		for {
			if _this.exitF {
				return
			}

			bts, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			var doc map[string]interface{}
			err = json.Unmarshal(bts, &doc)
			if err != nil {
				_this.errC <- err
				continue
			}

			for _, alias := range docSetup.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, "", "", _this.TaskId, doc, nil)
				op.SourceId = _this.Id
				op.DocumentSet = docSet     // 对应到表
				op.DocumentSetAlias = alias // 别名
				op.OptionType = msg.OptionTypeInsert
				opC <- op
			}
		}
	}
}

// Stream 流读取
func (_this *PlaintextReader) Stream(opC chan *msg.Op) {}

// Replica 副本模式
func (_this *PlaintextReader) Replica(opC chan *msg.Op) {}
