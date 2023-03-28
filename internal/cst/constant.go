package cst

const (
	SyncModeDirect  = "direct"  // 常规查询方式读取数据
	SyncModeStream  = "stream"  // 流读取事件
	SyncModeEmpty   = "empty"   // 不做任何事,只做连接
	SyncModeReplica = "replica" // 副本
)

func InSyncMode(t string) bool {
	switch t {
	case SyncModeDirect:
	case SyncModeStream:
	case SyncModeEmpty:
	case SyncModeReplica:
	default:
		return false
	}
	return true
}

const ResourceTypeEmpty = "empty"
const ResourceTypeMysql = "mysql"
const ResourceTypeElasticsearch = "elasticsearch"
const ResourceTypeRabbitMQ = "rabbitmq"
const ResourceTypePlaintext = "plaintext"

// InResourceType 支持的资源类型
func InResourceType(t string) bool {
	switch t {
	case ResourceTypeEmpty:
	case ResourceTypeMysql:
	case ResourceTypeElasticsearch:
	case ResourceTypeRabbitMQ:
	case ResourceTypePlaintext:
	default:
		return false
	}
	return true
}

// 任务状态
const (
	TaskNull = iota // 任务创建之后没有做任何操作
	TaskInit        // 任务初始化,检查资源配置项等操作
	TaskDone        // 任务自动结束或者手动结束
	TaskStop        // 任务处于停止状态-任务异常结束
	TaskRun         // 任务运行中
)
