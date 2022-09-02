package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/xwb1989/sqlparser"
	"math/rand"
	"runtime/debug"
	"strings"
	"time"
)

type MySQLWriter struct {
	WriterBaseParams                            // 继承了 Writer
	client           *client.Conn               // MySQL客户端连接
	latestConnTime   time.Time                  // 上一次连接时间
	tableStruct      map[string]map[string]bool // 保存表结构
}

// NewMySQLWriter 构建一个 mysql 连接
func NewMySQLWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *MySQLWriter {
	writer := new(MySQLWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc
	return writer
}

// clientInstance 获取一个有效的client,需要维护连接是否有效
func (_this *MySQLWriter) clientInstance(flush bool) (*client.Conn, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if !flush {
		if time.Now().Sub(_this.latestConnTime).Hours() >= 4 {
			flush = true
		} else if rand.Int()%10000 == 0 {
			flush = true
		}
	}
	var err error
	if flush {
		if cc == nil {
			cc, err = MySQLConn(_this.ResourceConf)
		} else {
			err = cc.Ping()
		}
		if err != nil {
			cc, err = MySQLConn(_this.ResourceConf)
			if err != nil {
				return nil, err
			}
		}
		_this.latestConnTime = time.Now()
		_this.client = cc
	}
	return cc, err
}

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *MySQLWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {

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
			case msg.CmdTypeMySQLDDL:
				err := _this.alterTableStruct(op.Cmd.Value)
				if err != nil {
					_this.errC <- fmt.Errorf("修改表结构错误:%s", err.Error())
				}
			}
			continue
		}

		// 设置写入超时
		failed := 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		go func(ctx context.Context) {
			defer func() {
				if e := recover(); e != nil {
					_this.errC <- errors.New(fmt.Sprintln(e))
					_this.errC <- errors.New(string(debug.Stack()))
					failed = len(ops)
				}
				cancel()
			}()
			failed = _this.Write(ops)
		}(ctx)
		select {
		case <-ctx.Done():
			call(failed)
		case <-time.After(timeout):
			call(len(ops))
			cancel()
			_this.errC <- errors.New("任务:" + _this.TaskId + ",写入超时")
			for _, op := range ops {
				errorMessage, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(errorMessage))
			}
		}
	}
	_this.exitWG.Done()
	_this.Release()
	_this.errC = nil
}

// Stop 停止
func (_this *MySQLWriter) Stop() {
	if _this.exitF {
		return
	}
	log.Info("taskId:" + _this.TaskId + " target stop")
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 释放资源
func (_this *MySQLWriter) Release() {
	if _this.client != nil {
		err := _this.client.Close()
		if err != nil {
			log.Info("taskId:" + _this.TaskId + " Release err:" + err.Error())
		}
		_this.client = nil
	}
	_this.DocumentSetMap = nil
}

// Write 写入
func (_this *MySQLWriter) Write(ops []*msg.Op) (failed int) {

	instance, err := _this.clientInstance(false)
	if err != nil {
		_this.errC <- fmt.Errorf("实例化连接对象错误:%s", err)
		return len(ops)
	}

	for _, op := range ops {
		docSet, _ := _this.DocumentSetMap[op.DocumentSetAlias]
		ns := strings.Split(docSet, ".")
		if len(ns) != 2 {
			_this.errC <- fmt.Errorf("解析数据库名字:%s 错误", docSet)
			_this.errC <- errors.New(op.DocJson())
			failed++
			continue
		}

		docIdField := op.DocIdField
		if docIdField == "" {
			docIdField, err = _this.getTablePK(ns[0], ns[1])
			if err != nil {
				_this.errC <- errors.New("获取表主键错误:" + err.Error())
				_this.errC <- errors.New(op.DocJson())
				failed++
				continue
			}
		}

		var str string
		switch op.OptionType {
		case msg.OptionTypeInsert: // 插入语句
			ks, vs := _this.buildInsertFV(op.Doc)
			str = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", ns[1], ks, vs)
		case msg.OptionTypeUpdate: // 修改语句,没有主键是修改不了的
			set, where, err := _this.buildUpdateFV(op, docIdField)
			if err != nil {
				_this.errC <- errors.New("获取表主键错误:" + err.Error())
				_this.errC <- errors.New(op.DocJson())
				failed++
				continue
			}
			str = fmt.Sprintf("UPDATE %s SET %s WHERE %s", ns[1], set, where)
		case msg.OptionTypeDelete: // 删除语句
			where := _this.buildDeleteFV(op, docIdField)
			str = fmt.Sprintf("DELETE FROM %s WHERE %s", ns[1], where)
		case msg.OptionTypeCustom:
		default:
			_this.errC <- errors.New("不支持的消息格式")
			_this.errC <- errors.New(op.DocJson())
			failed++
			continue
		}

		// 重试,主要是连接问题
		try := 3
		for i := 0; i < try; i++ {
			err = instance.UseDB(ns[0])
			if err == mysql.ErrBadConn {
				instance, err = _this.clientInstance(true)
			} else {
				break
			}
		}
		if err != nil {
			_this.errC <- err
			_this.errC <- errors.New(str)
			failed++
			continue
		}
		_, err = instance.Execute(str)
		if err != nil {
			switch e := err.(type) {
			case *mysql.MyError:
				_this.errC <- e
				_this.errC <- errors.New(str)
				failed++
			case error:
				// 连接错误
				if err == mysql.ErrBadConn {
					time.Sleep(time.Second)
					_, err = instance.Execute(str)
					if err != nil {
						_this.errC <- err
						_this.errC <- errors.New(str)
						failed++
					}
				}
			}
		}
	}
	return failed
}

// buildInsertFV 新增
func (_this *MySQLWriter) buildInsertFV(doc map[string]interface{}) (fieldStr string, valueStr string) {
	for field, value := range doc {
		fieldStr += fmt.Sprintf(",`%s`", field)
		switch v := value.(type) {
		case nil:
			valueStr += ",NULL"
		case string:
			valueStr += ",\"" + helper.SqlEscape(v) + "\""
		case []uint8:
			valueStr += ",\"" + helper.SqlEscape(string(v)) + "\""
		case float64, float32:
			valueStr += ",\"" + fmt.Sprintf("%.6f", v) + "\""
		case []interface{}, map[string]interface{}:
			bts, _ := json.Marshal(v)
			valueStr += ",\"" + helper.SqlEscape(string(bts)) + "\""
		default:
			valueStr += ",\"" + fmt.Sprint(v) + "\""
		}
	}
	fieldStr = strings.TrimLeft(fieldStr, ",")
	valueStr = strings.TrimLeft(valueStr, ",")
	return fieldStr, valueStr
}

// buildUpdateFV 凭借修改KV值和where
func (_this *MySQLWriter) buildUpdateFV(op *msg.Op, pkField string) (set string, where string, err error) {
	// 源是mysql,目标也是mysql,所以可以全等拼接
	if op.SourceFmt == msg.OpSourceFmtSQL {
		// 拼接 where
		value, _ := op.Doc[pkField]
		switch value := value.(type) {
		case nil:
			// 没有主键,数据不能修改
			return "", "", errors.New("主键不存在，不能修改数据")
		case []uint8:
			where = fmt.Sprintf("`%s`='%s' ", pkField, string(value))
		case string:
			where = fmt.Sprintf("`%s`='%s' ", pkField, value)
		case float64, float32:
			where = fmt.Sprintf("`%s`='%f.6' ", pkField, value)
		case uint8, int8, int, int32, int64, uint, uint32, uint64:
			where = fmt.Sprintf("`%s`='%d' ", pkField, value)
		default:
			where = fmt.Sprintf("`%s`='%s' ", pkField, fmt.Sprint(value))
		}

		// set
		for field, value := range op.Doc {
			if field == op.DocIdField {
				continue
			}
			switch v := value.(type) {
			case []uint8:
				set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(string(v)))
			case string:
				set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(v))
			case nil:
				set += fmt.Sprintf(",`%s`=NULL ", field)
			case uint8, int8, int, int32, int64, uint, uint32, uint64:
				set += fmt.Sprintf(",`%s`='%d' ", field, v)
			case float64, float32:
				set += fmt.Sprintf(",`%s`='%.6f' ", field, v)
			case []interface{}, map[string]interface{}:
				bts, _ := json.Marshal(v)
				set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(string(bts)))
			default:
				set += fmt.Sprintf(",`%s`='%s' ", field, fmt.Sprint(v))
			}
		}
		set = strings.TrimLeft(set, ",")
		return set, where, nil
	}

	// 需要处理 NOSQL 写入 SQL,NOSQl文档必有ID.但是纯文本咋整???
	where = fmt.Sprintf("%s='%s'", op.DocIdField, op.DocIdValue)
	for field, value := range op.Doc {
		if field == op.DocIdField {
			continue
		}
		switch v := value.(type) {
		case []uint8:
			where += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(string(v)))
		case string:
			where += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(v))
		case nil:
			where += fmt.Sprintf(",`%s`=NULL ", field)
		case uint8, int8, int, int32, int64, uint, uint32, uint64:
			where += fmt.Sprintf(",`%s`='%d' ", field, v)
		case float64, float32:
			where += fmt.Sprintf(",`%s`='%.6f' ", field, v)
		default:
			where += fmt.Sprintf(",`%s`='%s' ", field, fmt.Sprint(v))
		}
	}
	set = strings.TrimLeft(set, ",")
	where = strings.TrimLeft(where, ",")
	return set, where, nil
}

// buildDeleteFV 凭借修改KV值和where
func (_this *MySQLWriter) buildDeleteFV(op *msg.Op, pkField string) (where string) {
	// 源是mysql,目标也是mysql,所以可以全等拼接
	if op.SourceFmt == msg.OpSourceFmtSQL {
		// 拼接 where
		value, _ := op.Doc[pkField]
		switch value := value.(type) {
		case string:
			where = fmt.Sprintf("%s='%s'", pkField, value)
		case uint8, int8, int, int32, int64, uint, uint32, uint64:
			where = fmt.Sprintf("%s='%d'", pkField, value)
		default:
			where = fmt.Sprintf("%s='%s'", pkField, fmt.Sprint(value))
		}
		return
	}

	// 需要处理 NOSQL 写入 SQL,NOSQl文档必有ID.但是存文本咋整???
	where = fmt.Sprintf("%s='%s'", op.DocIdField, op.DocIdValue)
	return
}

// getTablePK 表主键
func (_this *MySQLWriter) getTablePK(db string, tab string) (pk string, err error) {

	stu, err := _this.descTableField(db, tab)
	if err != nil {
		return "", err
	}
	for f, isPk := range stu {
		if isPk {
			return f, nil
		}
	}
	return "", nil
}

// descTableField 表结构
func (_this *MySQLWriter) descTableField(db string, tab string) (stu map[string]bool, err error) {
	if _this.tableStruct == nil {
		_this.tableStruct = map[string]map[string]bool{}
	}
	ns := fmt.Sprintf("%s.%s", db, tab)
	stu, ok := _this.tableStruct[ns]
	if ok {
		return stu, nil
	}

	instance, err := _this.clientInstance(true)
	if err != nil {
		return nil, err
	}
	ts, _, err := MySQLTableStructField(instance, db, tab)
	if err != nil {
		return nil, err
	}
	_this.tableStruct[ns] = ts
	return ts, nil
}

// alterTableStruct 修改表结构
func (_this *MySQLWriter) alterTableStruct(v interface{}) (err error) {
	v1, ok := v.(map[string]string)
	if !ok || v1 == nil {
		err = fmt.Errorf(fmt.Sprint(v))
		return
	}
	sql, _ := v1["sql"]
	if sql == "" {
		err = fmt.Errorf("sql empty")
		return
	}

	stmt, _ := sqlparser.ParseStrictDDL(sql)
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok {
		err = fmt.Errorf("SQL语句解析错误,SQL语句为:%s", sql)
		return
	}
	dbName := ddl.Table.Qualifier.String()
	tbName := ddl.Table.Name.String()
	docSet := fmt.Sprintf("%s.%s", dbName, tbName)

	// 获取映射表
	mapTbName, _ := _this.DocumentSetMap[docSet]
	if mapTbName == "" {
		err = fmt.Errorf("文档集映射为空,文档集名称为:%s", docSet)
		return
	}
	mapSet := strings.Split(mapTbName, ".")
	if len(mapSet) != 2 {
		err = fmt.Errorf("文档集映射为空,文档集名称为:%s", mapSet)
		return
	}
	sql = strings.Replace(sql, dbName, mapSet[0], 1)
	sql = strings.Replace(sql, tbName, mapSet[1], 1)

	// 执行 sql
	instance, err := _this.clientInstance(true)
	if err != nil {
		return
	}
	_, err = instance.Execute(sql)
	if err != nil {
		err = fmt.Errorf("ddl:%s error:%s", sql, err.Error())
	} else {
		err = fmt.Errorf("ddl:%s success", sql)
	}
	_this.tableStruct = nil
	return
}
