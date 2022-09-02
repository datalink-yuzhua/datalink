package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"fmt"
	elastic7 "github.com/olivere/elastic/v7"
	"net/http"
	"time"
)

type ElasticsearchWriter struct {
	WriterBaseParams                  // 继承了 Writer
	client           *elastic7.Client // elastic客户端
	latestConnTime   time.Time        // 上一次连接时间
}

// NewElasticsearchWriter es写入
func NewElasticsearchWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *ElasticsearchWriter {
	writer := new(ElasticsearchWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc
	return writer
}

// client 获取client
func (_this *ElasticsearchWriter) clientInstance(flush bool) (*elastic7.Client, error) {
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
		_this.client = cc
	}
	return cc, nil
}

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *ElasticsearchWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)

	// 设置limit时,则开启批量写入. 最长5秒写入一次
	limitValue, _ := _this.TargetConf.Extra["limit"]
	limitF, _ := limitValue.(float64)
	limit := int(limitF)
	if limit < 1 {
		limit = 1
	}

	// 缓存队列,用于提高写入能力
	var needFlush bool
	var cacheList []*msg.Op // 缓存队列
	timeout := time.Second * 10
	t := time.NewTicker(timeout)
	for {
		if _this.exitF {
			_this.Write(cacheList)
			t.Stop()
			break
		}
		select {
		case ops := <-ch:

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

			cacheList = append(cacheList, ops...)

			// 计算是否需要执行写入
			if len(cacheList) >= limit {
				needFlush = true
			}
		case <-t.C:
			// 计算是否需要执行写入
			if len(cacheList) >= 0 {
				needFlush = true
			}
		}

		if !needFlush {
			continue
		}

		// 写入数据
		failed := 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		go func(ctx context.Context) {
			failed = _this.Write(cacheList)
			cancel()
		}(ctx)
		select {
		case <-ctx.Done():
			call(failed)
		case <-time.After(timeout):
			call(len(cacheList))
			cancel()
			_this.errC <- errors.New("任务:" + _this.TaskId + ",写入超时")
			for _, op := range cacheList {
				str, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(str))
			}
		}

		// reset
		cacheList = cacheList[:0]
		needFlush = false
	}
	_this.exitWG.Done()
	_this.Release()
	_this.errC = nil
}

// Stop 停止
func (_this *ElasticsearchWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 释放资源
func (_this *ElasticsearchWriter) Release() {
	log.Info("elasticsearch write stop")
	if _this.client != nil {
		_this.client.Stop()
		_this.client = nil
	}
	_this.DocumentSetMap = nil
}

// Write 设置批量刷写
func (_this *ElasticsearchWriter) Write(ops []*msg.Op) (failed int) {

	sum := len(ops)
	client, err := _this.clientInstance(false)
	if err != nil {
		client, err = _this.clientInstance(true)
		if err != nil {
			_this.errC <- errors.New(err.Error())
			return sum
		}
	}

	// 重试策略
	var rtf elastic7.RetrierFunc
	rtf = func(ctx context.Context, i int, request *http.Request, response *http.Response, err error) (time.Duration, bool, error) {
		if _this.exitF {
			return 0, false, errors.New("正在重试中，突然取消")
		}
		if i > 10 {
			return 0, false, err
		}
		return time.Second * time.Duration(i*i), true, nil
	}
	blk := client.Bulk().Retrier(rtf)

	// do
	var reqSrcArr []elastic7.BulkableRequest
	DoFunc := func() {
		defer func() {
			reqSrcArr = reqSrcArr[:0]
		}()

		resp, err := blk.Do(context.Background())
		if err != nil {
			s := _this.bodyAsString(ops)
			_this.errC <- fmt.Errorf("elasticsearch写入错误:%s \nbody:%s", err.Error(), s)
			failed += len(ops)
			return
		}
		if resp != nil {
			f := resp.Failed()
			for _, item := range f {
				s := _this.bodyAsString(ops)
				_this.errC <- fmt.Errorf("elasticsearch写入错误:%s \nbody:%s", item.Error.Reason, s)
			}
			failed += len(f)
		}
	}

	for _, op := range ops {

		// document
		doc := op.Doc

		// 获取索引,没有就使用读取的相关信息
		index, ok := _this.DocumentSetMap[op.DocumentSet]
		if !ok {
			index = op.DocumentSet
		}

		// 忽略掉 _id,将文档中的_id作为主键
		_id := op.DocIdValue
		if _id == "" {
			docId, _ := doc["_id"]
			_id, _ = docId.(string)
		}
		delete(doc, "_id")

		// OptionType
		switch op.OptionType {
		case msg.OptionTypeInsert:
			req := elastic7.NewBulkIndexRequest()
			req.Index(index)
			if _id != "" {
				req.Id(_id)
			}
			req.Doc(doc)
			blk.Add(req)
		case msg.OptionTypeUpdate:
			req := elastic7.NewBulkUpdateRequest()
			req.Index(index)
			if _id != "" {
				req.Id(_id)
			}
			req.Doc(doc)
			blk.Add(req)
		case msg.OptionTypeDelete:
			req := elastic7.NewBulkDeleteRequest()
			req.Index(index)
			if _id != "" {
				req.Id(_id)
			}
			blk.Add(req)
		}
	}

	// 一次刷写
	DoFunc()

	return failed
}

func (_this *ElasticsearchWriter) bodyAsString(ops []*msg.Op) string {
	var a []map[string]string
	for _, op := range ops {
		bs, _ := json.Marshal(op)
		a = append(a, map[string]string{
			op.OptionType: string(bs),
		})
	}
	str, _ := json.Marshal(a)
	return string(str)
}
