package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type RabbitMQWriter struct {
	WriterBaseParams                   // 继承了 Writer
	Conn             *amqp.Connection  // mq的连接
	channel          *amqp.Channel     // mq的channel
	extra            map[string]string // 其他参数
	latestConnTime   time.Time         // 上一次连接时间
	queName          string            // queue Name
	exName           string            // exchange Name
	rkName           string            // routing Key
}

// NewRabbitMQWriter RabbitMQ写入
func NewRabbitMQWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *RabbitMQWriter {
	writer := new(RabbitMQWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc

	writer.queName = writer.StrValue(tc.Extra, "queue_name")
	writer.exName = writer.StrValue(tc.Extra, "exchange")
	writer.rkName = writer.StrValue(tc.Extra, "routing_key")
	return writer
}

// clientInstance 连接实例
func (_this *RabbitMQWriter) clientInstance(flush bool) (*amqp.Channel, error) {
	cc := _this.Conn
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
		if cc == nil {
			cc, err = RabbitMQConn(_this.ResourceConf)
			_this.channel, err = cc.Channel()
		} else {
			if cc.IsClosed() {
				err = errors.New("close")
			}
		}
		if err != nil {
			cc, err = RabbitMQConn(_this.ResourceConf)
			_this.channel, err = cc.Channel()
			if err != nil {
				return nil, err
			}
		}
		_this.latestConnTime = time.Now()
		_this.Conn = cc
		if err != nil {
			return nil, err
		}
	}
	return _this.channel, nil
}

// Run 写入目标数据
func (_this *RabbitMQWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)

	var needFlush bool
	var cacheList []*msg.Op // 缓存队列
	timeout := time.Second * 10
	tk := time.NewTicker(timeout)
	for {
		if _this.exitF {
			if len(cacheList) > 0 {
				_this.Write(cacheList)
			}
			tk.Stop()
			break
		}

		select {
		case ops := <-ch:
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
			needFlush = true
		case <-tk.C:
			if len(cacheList) >= 0 {
				needFlush = true
			}
		}

		if !needFlush {
			continue
		}

		// 设置写入超时
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
			_this.errC <- errors.New("taskId:" + _this.TaskId + ", timeout write")
			for _, op := range cacheList {
				msg, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(msg))
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
func (_this *RabbitMQWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 清理资源
func (_this *RabbitMQWriter) Release() {
	log.Info("消息队列写入关闭")
	if _this.channel != nil {
		_this.channel.Close()
		_this.channel = nil
	}
	if _this.Conn != nil && !_this.Conn.IsClosed() {
		_this.Conn.Close()
		_this.Conn = nil
	}
	_this.DocumentSetMap = nil
	_this.extra = nil
}

// Write 向mq中写入
func (_this *RabbitMQWriter) Write(ops []*msg.Op) (failed int) {

	// 是否需要格式化为json格式
	f, _ := _this.TargetConf.Extra["format"]
	format, _ := f.(bool)

	ch, err := _this.clientInstance(false)
	if err != nil {
		_this.errC <- err
		return len(ops)
	}

	// 不识别其他操作
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, op := range ops {
		var msgBody map[string]interface{}
		if format {
			msgBody = map[string]interface{}{"OptionType": op.OptionType, "data": op.Doc}
		} else {
			msgBody = op.Doc
		}

		bytes, err := json.Marshal(msgBody)
		if err != nil {
			_this.errC <- err
			failed++
			continue
		}
		//{
		//	"OptionType":"insert",
		//	"data":""
		//}
		// retry 3
		err = ch.PublishWithContext(
			ctx,
			_this.exName,
			_this.rkName,
			false,
			false,
			amqp.Publishing{
				Body: bytes,
			},
		)
		if err != nil {
			failed++
			_this.errC <- err
		}
	}
	return
}

// StrValue 格式化值
func (_this *RabbitMQWriter) StrValue(m map[string]interface{}, k string) string {
	v, ok := m[k]
	if !ok {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case []rune:
		return string(v)
	case nil:
		return ""
	case float64, float32:
		return fmt.Sprintf("%f", v)
	default:
		return fmt.Sprint(v)
	}
}
