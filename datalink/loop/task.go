package loop

import (
	"data-link-2.0/datalink/terms"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"errors"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	Conf           conf.TaskConf                  // 配置
	Uuid           string                         // 唯一标识
	State          int                            // 任务状态
	resM           map[string]conf.Resource       // 资源连接对象
	readerM        map[string]terms.ReaderRunImp  // 数据源
	writerM        map[string]terms.WriterTermImp // 目标源
	taskWG         sync.WaitGroup                 // 任务的wg
	pipeM          map[string]interface{}         // pipeline
	readOpC        chan *msg.Op                   // task的读取
	ReadDone       bool                           // 读取完成
	ReadDoneCount  int                            // 读取完成
	writeOpCMap    map[string]chan []*msg.Op      // 路由多个target写入
	WriteDone      bool                           // 写入完成
	WriteDoneCount int                            // 写入完成
	ErrC           chan error                     // 错误日志
	errF           bool                           // 错误日志退出
	exitF          bool                           // 退出

	// 任务统计
	Ts *TaskStatistic // 任务统计

	// pipeline
	pipelines map[string]*Pipeline // pipeline 数据列表

	// space
	bufferSpace               []map[string]interface{} // 缓存空间
	spaceLimitChangeState     bool                     // space状态变更
	spaceLimitChangeStateLock sync.Mutex               // space状态锁
}

func NewTask(tc conf.TaskConf, id string) *Task {
	// 生成 uuid
	if id == "" {
		log.Error("need task id")
		return nil
	}

	t := new(Task)
	t.Conf = tc
	t.Uuid = id
	t.State = cst.TaskNull
	t.resM = make(map[string]conf.Resource)
	t.readerM = make(map[string]terms.ReaderRunImp)
	t.writerM = make(map[string]terms.WriterTermImp)
	t.pipeM = make(map[string]interface{})
	t.readOpC = make(chan *msg.Op)
	t.writeOpCMap = map[string]chan []*msg.Op{}
	t.ReadDone = false
	t.WriteDone = false
	t.Ts = NewTaskStatistic(t.Uuid)
	t.pipelines = make(map[string]*Pipeline)
	t.ErrC = make(chan error)
	t.errF = false
	t.exitF = false
	return t
}

// Init 初始化数据
func (_this *Task) Init() bool {

	// 连接资源
	_this.State = cst.TaskInit
	r := _this.loadResource()
	if !r {
		_this.ErrC <- errors.New("loadResource Error")
		_this.State = cst.TaskStop
		return false
	}
	r = _this.buildSource()
	if !r {
		_this.ErrC <- errors.New("buildSource Error")
		_this.State = cst.TaskStop
		return false
	}
	r = _this.buildTarget()
	if !r {
		_this.ErrC <- errors.New("buildTarget Error")
		_this.State = cst.TaskStop
		return false
	}

	// 注册Flow
	p := NewPipeline(_this.Uuid, _this.Conf.Pipelines)
	_, err := RegisterFlow(p, _this)
	if err != nil {
		_this.ErrC <- err
		return false
	}

	return true
}

// Run 任务运行
func (_this *Task) Run() {
	// 运行错误处理
	_this.runErrorChan()

	// 加载资源
	if _this.Init() == false {
		_this.stopErrorChan()
		return
	}

	// 统计
	_this.Ts.Run(5)

	// 写入
	_this.runTarget()

	// 读取
	_this.runSource()

	// pipeline
	_this.runDirectPipeline()

	// 运行状态
	_this.State = cst.TaskRun

	// 控制退出
	go func() {
		tk := time.NewTicker(2 * time.Second)
		defer func() {
			if e := recover(); e != nil {
				_this.State = cst.TaskStop
				log.Info(e)
			}
			tk.Stop()
		}()
		idx := 0
		for {
			select {
			case <-tk.C:
				if _this.exitF {
					_this.stopSource()
				}
				// 标记状态
				if _this.ReadDone && _this.WriteDone {
					mu := sync.RWMutex{}
					mu.Lock()
					_this.Release()
					if _this.State != cst.TaskDone && _this.State != cst.TaskStop {
						_this.State = cst.TaskDone
					}
					mu.Unlock()
					return
				}
				if idx%10 == 0 {
					log.Info("taskId:" + _this.Uuid + " running")
				}
				idx++
			}
		}
	}()
}

// Release 释放资源
func (_this *Task) Release() {
	log.Info("taskId:" + _this.Uuid + " stop in")

	for _, imp := range _this.readerM {
		imp.Clear()
	}
	// stop:error,stat
	_this.stopErrorChan()
	_this.Ts.Stop()
	RemoveFlow(_this.Uuid)

	close(_this.readOpC)
	_this.readOpC = nil
	_this.writeOpCMap = nil

	_this.resM = nil
	_this.readerM = nil
	_this.writerM = nil

	log.Info("taskId:" + _this.Uuid + " stop end")
}

// Stop 任务停止
func (_this *Task) Stop() {
	_this.exitF = true
	_this.taskWG.Wait()
}

// loadResource 构建连接资源
func (_this *Task) loadResource() bool {
	l := len(_this.Conf.Resources)
	if l < 1 {
		return false
	}
	for _, v := range _this.Conf.Resources {
		_this.resM[v.Id] = v
	}
	return true
}

// buildTarget 构建target
func (_this *Task) buildTarget() bool {
	l := len(_this.Conf.Targets)
	if l < 1 {
		return false
	}
	_this.writerM = make(map[string]terms.WriterTermImp, l)
	for i, v := range _this.Conf.Targets {

		_, ok := _this.writerM[v.ResourceId]
		if ok {
			_this.ErrC <- errors.New("target resourceId need unique")
			return false
		}

		s := _this.resM[v.ResourceId]
		var imp terms.WriterTermImp
		switch s.Type {
		case cst.ResourceTypeEmpty:
			imp = terms.NewEmptyWriter(i, _this.Uuid, s, v)
		case cst.ResourceTypeMysql:
			imp = terms.NewMySQLWriter(i, _this.Uuid, s, v)
		case cst.ResourceTypeElasticsearch:
			imp = terms.NewElasticsearchWriter(i, _this.Uuid, s, v)
		case cst.ResourceTypeRabbitMQ:
			imp = terms.NewRabbitMQWriter(i, _this.Uuid, s, v)
		default:
			_this.ErrC <- errors.New("no support target type")
			return false
		}
		_this.writerM[v.ResourceId] = imp

		chw := make(chan []*msg.Op)
		for readerSet, _ := range v.DocumentSet {
			_this.writeOpCMap[readerSet] = chw
		}
	}
	return true
}

// build 建立连接对象
func (_this *Task) buildSource() bool {
	l := len(_this.Conf.Sources)
	if l < 1 {
		return false
	}
	_this.readerM = make(map[string]terms.ReaderRunImp, l)
	for i, v := range _this.Conf.Sources {
		s := _this.resM[v.ResourceId]
		var imp terms.ReaderRunImp
		switch s.Type {
		case cst.ResourceTypeEmpty:
			imp = terms.NewEmptyReader(i, _this.Uuid, s, v)
		case cst.ResourceTypeMysql:
			imp = terms.NewMySQLReader(i, _this.Uuid, s, v)
		case cst.ResourceTypeElasticsearch:
			imp = terms.NewElasticsearchReader(i, _this.Uuid, s, v)
		case cst.ResourceTypeRabbitMQ:
			imp = terms.NewRabbitMQReader(i, _this.Uuid, s, v)
		default:
			_this.ErrC <- errors.New("no support source type")
			return false
		}
		_this.readerM[v.ResourceId] = imp
	}
	return true
}

// GetSourceImp 通过内部索引取imp
func (_this *Task) GetSourceImp(resourceId string) (v terms.ReaderRunImp, ok bool) {
	v, ok = _this.readerM[resourceId]
	return v, ok
}

// runTarget
func (_this *Task) runTarget() {
	log.Info("运行写入")
	for _, v := range _this.writerM {
		_this.taskWG.Add(1)
		go func(imp terms.WriterTermImp) {
			defer func() {
				if e := recover(); e != nil {
					_this.State = cst.TaskStop
					_this.ErrC <- errors.New(fmt.Sprint(e))
				}
				_this.taskWG.Done()
				_this.WriteDoneCount++
				if len(_this.writerM) == _this.WriteDoneCount {
					_this.WriteDone = true
				}
			}()

			var ds map[string]string
			switch a := imp.(type) {
			case *terms.EmptyWriter:
				ds = a.TargetConf.DocumentSet
			case *terms.MySQLWriter:
				ds = a.TargetConf.DocumentSet
			case *terms.ElasticsearchWriter:
				ds = a.TargetConf.DocumentSet
			case *terms.RabbitMQWriter:
				ds = a.TargetConf.DocumentSet
			}

			var ok bool
			var ch chan []*msg.Op
			for s, _ := range ds {
				ch, ok = _this.writeOpCMap[s]
				if ok {
					break
				}
			}
			imp.Run(ch, _this.ErrC, func(failed int) {
				atomic.AddInt64(&_this.Ts.FailedDeliveryCount, int64(failed))
			})
		}(v)
	}
}

// runSource 读取事件
func (_this *Task) runSource() {
	log.Info("运行读取")
	for _, v := range _this.readerM {
		_this.taskWG.Add(1)
		go func(imp terms.ReaderRunImp) {
			defer func() {
				if e := recover(); e != nil {
					_this.State = cst.TaskStop
					_this.ErrC <- errors.New(fmt.Sprint(e))
				}

				// 异常信息
				e1 := imp.Err()
				if e1 != nil {
					_this.State = cst.TaskStop
					_this.ErrC <- e1
				}

				_this.taskWG.Done()
				_this.ReadDoneCount++
				if _this.ReadDoneCount == len(_this.readerM) {
					_this.ReadDone = true
					op := msg.NewCmdOp(_this.Uuid, msg.CmdTypeReadDone, true)
					_this.readOpC <- op
				}
			}()

			imp.Run(_this.readOpC, _this.ErrC)
		}(v)
	}
}

// runPipeline
func (_this *Task) runDirectPipeline() {
	log.Info("运行工作台")
	go func() {
		tk := time.NewTicker(2 * time.Second)
		defer func() {
			if e := recover(); e != nil {
				_this.State = cst.TaskStop
				_this.ErrC <- errors.New(fmt.Sprint(e))
			}
			tk.Stop()
		}()

		for {
			select {
			case <-tk.C:
				// 不可使用t.exitF判断退出
				if _this.ReadDone && _this.WriteDone {
					log.Info("工作台关闭")
					return
				}
			case op, ok := <-_this.readOpC:
				if !ok {
					continue
				}
				// 命令类消息
				if op.MessageType == msg.MessageTypeCmd {
					_this.routeOpToWriterTerm([]*msg.Op{op})
					continue
				}
				atomic.AddInt64(&_this.Ts.FullReadCount, 1)

				// 普通消息
				ops, err := RunningFlow(op, _this)
				if err != nil {
					atomic.AddInt64(&_this.Ts.FailedPipeLineCount, 1)
					_this.ErrC <- fmt.Errorf("pipeline flow error:%s", err.Error())
					continue
				}
				var sendOps []*msg.Op
				for _, op := range ops {
					if op == nil {
						continue
					}
					if op.MessageType == msg.MessageTypeCmd {
						continue
					}
					if op.IsDel {
						continue
					}
					sendOps = append(sendOps, op)
				}
				_this.routeOpToWriterTerm(sendOps)
			}
		}
	}()
}

// runErrorChan 运行一个errorChan 用于记录,读写错误
func (_this *Task) runErrorChan() {
	log.Info("运行错误管道")
	go func() {
		tk := time.NewTicker(2 * time.Second)
		defer func() {
			if e := recover(); e != nil {
				log.Error(e)
			}
			tk.Stop()
			log.Info("错误管道关闭")
		}()

		// 初始化日志
		er, _ := _this.Conf.Setup["error_record"]
		recordErr, _ := er.(bool)
		var logger *logrus.Logger
		if recordErr {
			filePath := conf.G.TaskErrorPath + _this.Uuid + ".log"
			logger = logrus.New()
			logger.SetFormatter(&logrus.TextFormatter{})

			// 切割文件,保留30天,每天一个新文件,每个文件最大100M
			w, _ := rotatelogs.New(
				filePath+".%Y%m%d",
				rotatelogs.WithClock(rotatelogs.UTC),
				rotatelogs.WithLinkName(filePath),
				rotatelogs.WithMaxAge(time.Duration(30*24)*time.Hour),
				rotatelogs.WithRotationSize(100*1024*1024), // 100M切割文件大小
				rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
			)
			logger.SetOutput(w)
		}

		for {
			select {
			case <-tk.C:
				if _this.errF {
					return
				}
			case err := <-_this.ErrC:
				if logger != nil {
					logger.Error(err.Error())
				}
			}
		}
	}()
}

// stopSource 停止源读取
func (_this *Task) stopSource() {
	for _, imp := range _this.readerM {
		imp.Stop()
	}
}

// StopErrorChan 退出error
func (_this *Task) stopErrorChan() {
	_this.errF = true
}

// routeOpToWriterTerm 路由1
func (_this *Task) routeOpToWriterTerm(ops []*msg.Op) {
	for _, op := range ops {
		// 处理命令类的消息
		// 需要查询对应的写入终端,结束了
		if op.MessageType == msg.MessageTypeCmd {
			// 由于是别名,所以有重复的可能,通过map去掉重复的chan
			d := map[chan []*msg.Op]bool{}
			for _, c := range _this.writeOpCMap {
				_, ok := d[c]
				if ok {
					continue
				}
				c <- []*msg.Op{op}
				d[c] = true
			}
			continue
		}

		c, ok := _this.writeOpCMap[op.DocumentSetAlias]
		if ok {
			c <- []*msg.Op{op}
		}
	}
}

// DisplayState 将状态转为字符串显示
func DisplayState(State int) string {
	var stateStr string
	switch State {
	case cst.TaskNull: // 任务创建之后没有做任何操作
		stateStr = "null"
	case cst.TaskInit: // 任务初始化
		stateStr = "init"
	case cst.TaskDone: // 任务主动结束或者手动结束
		stateStr = "done"
	case cst.TaskStop: // 任务异常结束
		stateStr = "stop"
	case cst.TaskRun: // 任务运行中
		stateStr = "running"
	default:
		stateStr = "unknown state"
	}
	return stateStr
}

// Display 以map显示信息
func (_this *Task) Display() map[string]interface{} {
	stateStr := DisplayState(_this.State)

	var desc string
	if len(_this.Conf.Setup) > 0 {
		desc, _ = _this.Conf.Setup["desc"].(string)
	}
	var readMode string
	if len(_this.Conf.Sources) > 0 {
		readMode = _this.Conf.Sources[0].SyncMode
	}
	msg1 := _this.Ts.Display(_this.State)

	hss := "none"
	if _this.State == cst.TaskStop {
		hss = "red"
	} else if _this.State == cst.TaskRun {
		if _this.Ts.UnitFailed() {
			hss = "yellow"
		} else {
			hss = "green"
		}
	}
	msg2 := map[string]interface{}{
		"Uuid":           _this.Uuid,
		"State":          stateStr,
		"HealthState":    hss,
		"Desc":           desc,
		"ReadDone":       _this.ReadDone,
		"SyncMode":       readMode,
		"WriteDone":      _this.WriteDone,
		"LatestRunError": "",
	}
	return helper.MapsMerge(msg1, msg2)
}

// DisplayErrors 显示错误文件内容
// sort asc顺序,desc倒序
// line -1读取所有数据, >0读取指定行数
func (_this *Task) DisplayErrors(sort string, lineCount int) string {
	// 由于是symlink文件,所以需要真实地址.
	file := conf.G.TaskErrorPath + _this.Uuid + ".log"
	readlink, err := os.Readlink(file)
	if err != nil {
		return err.Error()
	}
	file = conf.G.TaskErrorPath + readlink
	return helper.ReadFileByLine(file, sort, lineCount)
}

// RemoveResumeFile 移除resume文件
func (_this *Task) RemoveResumeFile() {
	path := conf.G.TaskResumePath
	if _, err := helper.PathExists(path); err != nil {
		return
	}

	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}

	sep := string(os.PathSeparator)
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}
		if strings.HasPrefix(fi.Name(), _this.Uuid) {
			file := path + sep + fi.Name()
			os.Remove(file)
		}
	}
}
