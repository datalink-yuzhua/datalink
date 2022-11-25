package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type RabbitMQReader struct {
	ReaderBaseParams                  // 继承了 Reader
	client           *amqp.Connection // mq客户端
	ch               *amqp.Channel    // channel
	latestConnTime   time.Time        // 上一次连接时间
	consumerTag      string           // consumerTag
}

// NewRabbitMQReader rabbit 客户端
func NewRabbitMQReader(id int, tid string, rc conf.Resource, sc conf.Reader) *RabbitMQReader {
	reader := new(RabbitMQReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	reader.consumerTag = fmt.Sprintf("datalink-consumer-%d", time.Now().Unix())
	return reader
}

// clientInstance 连接实例
func (_this *RabbitMQReader) clientInstance(flush bool) (*amqp.Connection, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if !flush {
		if time.Now().Sub(_this.latestConnTime).Hours() >= 4 {
			flush = true
		}
	}
	var err error
	if flush {
		if cc != nil && !cc.IsClosed() {
			cc.Close()
		}
		cc, err = RabbitMQConn(_this.ResourceConf)
		if err != nil {
			cc, err = RabbitMQConn(_this.ResourceConf)
			if err != nil {
				return nil, err
			}
		}
		_this.latestConnTime = time.Now()
		_this.client = cc
		_this.ch, _ = cc.Channel()
	}
	return cc, err
}

// Run 运行任务
func (_this *RabbitMQReader) Run(opC chan *msg.Op, errC chan error) {
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
			// 不需要释放对象,需要处理
		}
		_this.exitF = true
	}()

	// 当为stream时需要监控退出
	if _this.SourceConf.SyncMode == cst.SyncModeStream {
		_this.exitWG.Add(1)
		go func() {
			defer func() {
				_this.exitWG.Done()
			}()

			tk := time.NewTicker(2 * time.Second)
			for {
				select {
				case <-tk.C:
					if _this.exitF {
						tk.Stop()
						if _this.ch != nil {
							// 关闭channel
							_this.ch.Cancel(_this.consumerTag, true)
						}
						return
					}
				}
			}
		}()
	}

	// 等待读取结束
	_this.exitWG.Wait()
	_this.Clear()
}

// Stop 停止任务
func (_this *RabbitMQReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *RabbitMQReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 清理资源
func (_this *RabbitMQReader) Clear() {
	_this.Release()
}

// Release 释放资源
func (_this *RabbitMQReader) Release() {
	if _this.ch != nil && !_this.ch.IsClosed() {
		_this.ch.Close()
		_this.ch = nil
	}
	if _this.client != nil && !_this.client.IsClosed() {
		_this.client.Close()
		_this.client = nil
	}
	_this.errC = nil
}

// SyncMode 同步模式
func (_this *RabbitMQReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
func (_this *RabbitMQReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *RabbitMQReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// Stream 流读取
func (_this *RabbitMQReader) Stream(opC chan *msg.Op) {
	_, err := _this.clientInstance(true)
	if err != nil {
		_this.err = err
		return
	}
	ch := _this.ch

	// 不定义queue,直接从queue中取值
	que := _this.fmtValue(_this.SourceConf.Extra, "queue_name")
	rk := _this.fmtValue(_this.SourceConf.Extra, "routing_key")
	ex := _this.fmtValue(_this.SourceConf.Extra, "exchange")

	// 主键字段,不然不能更新
	pkField := _this.fmtValue(_this.SourceConf.Extra, "pk_field")

	err = ch.QueueBind(que, rk, ex, false, nil)
	if err != nil {
		_this.err = err
		return
	}
	deliveryC, err := ch.Consume(
		que,
		_this.consumerTag,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		_this.err = err
		return
	}
	tk := time.NewTicker(5 * time.Second)
	for {
		if _this.exitF {
			tk.Stop()
			return
		}
		var msgBody map[string]interface{}
		select {
		case d, ok := <-deliveryC:
			if !ok {
				continue
			}
			err := json.Unmarshal(d.Body, &msgBody)
			if err != nil {
				_this.errC <- err
				continue
			}
			if msgBody == nil {
				_this.errC <- errors.New("mq body error:" + string(d.Body))
				continue
			}
		case <-tk.C:
			continue
		}
		//标准数据格式,如果不是该结构则是为插入操作
		//{
		//	"OptionType":"insert",
		//	"data":""
		//}
		//或则
		//{}
		var doc map[string]interface{}
		ot := msg.OptionTypeInsert
		ots, ok := msgBody["OptionType"]
		if ok {
			switch ots {
			case "insert":
				ot = msg.OptionTypeInsert
			case "update":
				ot = msg.OptionTypeUpdate
			case "delete":
				ot = msg.OptionTypeDelete
			default:
				ot = msg.OptionTypeInsert
			}
			msgData, ok := msgBody["data"]
			if !ok {
				_this.errC <- errors.New("mq message format error")
				continue
			}
			doc, _ = msgData.(map[string]interface{})
		} else {
			doc = msgBody
			ot = msg.OptionTypeInsert
		}
		if doc == nil {
			_this.errC <- errors.New("mq body data error")
			continue
		}

		for docSet, docSetup := range _this.SourceConf.DocumentSet {
			for _, alias := range docSetup.Alias {
				pkValue := _this.fmtValue(msgBody, pkField)
				op := msg.NewDocOp(msg.OpSourceFmtNOSQL, pkValue, pkField, _this.TaskId, doc, nil)
				op.SourceId = _this.Id
				op.DocumentSet = docSet
				op.DocumentSetAlias = alias
				op.OptionType = ot
				opC <- op
			}
		}
	}
}

// Direct 常规方式查表导数据
func (_this *RabbitMQReader) Direct(opC chan *msg.Op) {}

// Replica 副本模式
func (_this *RabbitMQReader) Replica(opC chan *msg.Op) {}

// fmtValue 格式化值
func (_this *RabbitMQReader) fmtValue(m map[string]interface{}, k string) string {
	v, ok := m[k]
	if !ok {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case []uint8:
		return string(v)
	case nil:
		return ""
	case float64, float32:
		return fmt.Sprintf("%f", v)
	default:
		return fmt.Sprint(v)
	}
}
