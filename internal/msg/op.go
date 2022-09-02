package msg

import (
	"encoding/json"
)

const (
	MessageTypeCmd = iota // 命令类型
	MessageTypeDoc        // 文档类型
)

const (
	OpSourceFmtSQL   = iota // MySQL MariaDB
	OpSourceFmtNOSQL        // MongoDB,Elasticsearch
)

const (
	OptionTypeInsert = "insert" // 文档类型 - 新增
	OptionTypeUpdate = "update" // 文档类型 - 修改
	OptionTypeDelete = "delete" // 文档类型 - 删除
	OptionTypeCustom = "custom" // 文档类型 - 自定义
)
const (
	CmdTypeReadDone = "MsgReadState" // 已经读取完毕
	CmdTypeMySQLDDL = "MySQLDDL"     // MySQL的表结构变更
)

// ObsMsg Obs的消息体
type ObsMsg struct {
	Project string                   // 项目名称
	Data    []map[string]interface{} // 数据集
}

// Op 消息转换
type Op struct {
	TaskId   string // 用于指向task,用于 pipeline 处理
	SourceId int    // 用于指向task,用于 pipeline 处理

	// Cmd 类型命令
	Cmd         *Cmd // 命令数据
	MessageType int  // 消息类型

	// row消息类型
	SourceFmt        int                    // NOSQL SQL
	DocumentSet      string                 // 指向 mydb.coll
	DocumentSetAlias string                 // 别名,用于数据处理
	Doc              map[string]interface{} // 新文档数据
	DocIdValue       string                 // 新文档数据,json文档的ID
	DocIdField       string                 // 新文档数据,json文档的ID
	OriginDoc        map[string]interface{} // 原始文档,有主键的功能
	OptionType       string                 // 操作类型

	// 消息类型
	IsDel bool // 是否标记删除
}

type Cmd struct {
	Type  string
	Value interface{}
}

// DocJson 格式化json文档
func (o Op) DocJson() string {
	if o.Doc == nil {
		return ""
	}
	b, _ := json.Marshal(o.Doc)
	return string(b)
}

// NewDocOp 新建一条 doc Op
func NewDocOp(sourceFmt int, docIdValue string, docIdField string, taskId string, doc map[string]interface{}, originDoc map[string]interface{}) *Op {
	return &Op{
		SourceFmt:   sourceFmt,
		DocIdValue:  docIdValue,
		DocIdField:  docIdField,
		Doc:         doc,
		TaskId:      taskId,
		MessageType: MessageTypeDoc,
		OriginDoc:   originDoc}
}

// NewCmdOp 新建一条 cmd Op
func NewCmdOp(taskId string, tp string, val interface{}) *Op {
	c := new(Cmd)
	c.Type = tp
	c.Value = val
	return &Op{Cmd: c, TaskId: taskId, MessageType: MessageTypeCmd}
}
