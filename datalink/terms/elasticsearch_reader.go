package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/msg"
	"encoding/json"
	elastic7 "github.com/olivere/elastic/v7"
	"io"
	"time"
)

type ElasticsearchReader struct {
	ReaderBaseParams                  // 继承了 Reader
	client           *elastic7.Client // elastic客户端
	latestConnTime   time.Time        // 上一次连接时间
}

// NewElasticsearchReader 构建es客户端
func NewElasticsearchReader(id int, tid string, rc conf.Resource, sc conf.Reader) *ElasticsearchReader {
	reader := new(ElasticsearchReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	return reader
}

// clientInstance 获取实例,走的是http接口
func (_this *ElasticsearchReader) clientInstance(flush bool) (*elastic7.Client, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if !flush {
		if time.Now().Sub(_this.latestConnTime).Hours() >= 4 {
			flush = true
		}
	}
	if flush {
		var err error
		if cc != nil && cc.IsRunning() {
			cc.Stop()
		}
		cc, err = ElasticSearchConn(_this.ResourceConf)
		if err != nil {
			return nil, err
		}
		_this.latestConnTime = time.Now()
		_this.client = cc
	}
	return cc, nil
}

// Run 运行任务
func (_this *ElasticsearchReader) Run(opC chan *msg.Op, errC chan error) {
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
func (_this *ElasticsearchReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *ElasticsearchReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *ElasticsearchReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *ElasticsearchReader) Release() {
	if _this.client != nil {
		_this.client.Stop()
	}
	_this.client = nil
}

// SyncMode 同步模式
func (_this *ElasticsearchReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
func (_this *ElasticsearchReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *ElasticsearchReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// Direct 读取数据
func (_this *ElasticsearchReader) Direct(opC chan *msg.Op) {
	instance, err := _this.clientInstance(true)
	if err != nil {
		_this.err = err
		return
	}

	limitValue, _ := _this.SourceConf.Extra["limit"]
	limit, _ := limitValue.(float64)
	if limit < 0 {
		limit = 100
	}
	for docSet, docSetup := range _this.SourceConf.DocumentSet {
		scroll := instance.Scroll(docSet).Size(int(limit))
		for {
			results, err := scroll.Do(context.Background())
			if err != nil {
				if err == io.EOF {
					break
				}
				_this.errC <- err
				continue
			}
			if _this.exitF {
				return
			}
			for _, hit := range results.Hits.Hits {

				var doc map[string]interface{}
				err := json.Unmarshal(hit.Source, &doc)
				if err != nil {
					_this.errC <- err
					continue
				}

				for _, alias := range docSetup.Alias {
					op := msg.NewDocOp(msg.OpSourceFmtNOSQL, hit.Id, "_id", _this.TaskId, doc, nil)
					op.SourceId = _this.Id
					op.DocumentSet = hit.Index
					op.DocumentSetAlias = alias
					op.OptionType = msg.OptionTypeInsert
					opC <- op
				}
			}
		}
	}
}

// Stream 流读取
func (_this *ElasticsearchReader) Stream(opC chan *msg.Op) {}

// Replica 副本模式
func (_this *ElasticsearchReader) Replica(opC chan *msg.Op) {}
