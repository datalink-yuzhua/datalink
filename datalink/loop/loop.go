package loop

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Loop struct {
	startAt   time.Time                   // 启动时间
	tasks     map[string]*Task            // 任务列表
	exitC     chan bool                   // 退出信号
	exitWG    sync.WaitGroup              // 退出WG
}

var LOOP *Loop

func NewLoop() *Loop {
	log.Info("任务池初始化")
	loop := new(Loop)
	loop.startAt = time.Now()
	loop.tasks = make(map[string]*Task)
	loop.exitC = make(chan bool)
	LOOP = loop
	return loop
}

// Start 启动服务
func (_this *Loop) Start() {
	log.Info("任务池启动")

	_this.exitWG.Add(1)
	go func() {
		tk := time.NewTicker(2 * time.Second)
		defer func() {
			tk.Stop()
			_this.exitWG.Done()
		}()

		for {
			select {
			case <-_this.exitC:
				return
			case <-tk.C:
			}
		}
	}()
}

// Wait 等待服务结束
func (_this *Loop) Wait() {
	log.Info("loop Wait")
	_this.exitWG.Wait()
}

// Stop 任务结束,立即返回
func (_this *Loop) Stop() {
	log.Info("任务池停止")
	for _, t := range _this.TaskList() {
		if t.State == cst.TaskRun {
			t.Stop()
		}
	}
}

// AddTask 添加一个任务
func (_this *Loop) AddTask(task *Task) (ok bool, err error) {
	_, ok = _this.tasks[task.Uuid]
	if ok {
		return false, errors.New("任务id重复，任务id:" + task.Uuid)
	}
	_this.tasks[task.Uuid] = task
	log.Info("添加任务,任务id:" + task.Uuid)
	return true, nil
}

// RunTask 启动一个任务
func (_this *Loop) RunTask(uuid string) (ok bool, err error) {
	oldTask, ok := _this.tasks[uuid]
	if !ok {
		return false, errors.New("任务不存在")
	}
	if oldTask.State == cst.TaskRun || oldTask.State == cst.TaskInit {
		return false, errors.New("任务运行中")
	}
	delete(_this.tasks, uuid)

	newTask := NewTask(oldTask.Conf, uuid)
	_this.tasks[uuid] = newTask
	newTask.Run()

	log.Info("运行任务,任务id:" + uuid)
	return true, nil
}

// StopTask 停止一个任务
func (_this *Loop) StopTask(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	if !ok {
		return false, errors.New("任务不存在")
	}
	if t.State != cst.TaskRun && t.State != cst.TaskStop {
		if t.State == cst.TaskInit {
			return false, errors.New("任务初始化中，不能停止")
		}
		return false, errors.New("任务没有运行")
	}

	t.Stop()

	log.Info("停止任务,任务id:" + uuid)
	return true, nil
}

// RemoveTask 删除任务时,也会删除文件
func (_this *Loop) RemoveTask(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	if !ok {
		return true, errors.New("任务没找到")
	}

	// 任务是否可移除
	if t.State == cst.TaskRun || t.State == cst.TaskInit {
		return false, errors.New("当前任务状态不能移除")
	}

	_, err = _this.RemoveTaskFile(uuid)
	if err != nil {
		return false, err
	}
	RemoveFlow(t.Uuid)
	delete(_this.tasks, uuid)

	log.Info("移除任务,任务id:" + uuid)
	return true, nil
}

// RemoveTaskFile 移除任务相关文件
func (_this *Loop) RemoveTaskFile(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	c := conf.G

	// 删除日志文件,日志文件是切割
	err = filepath.Walk(c.TaskErrorPath, func(path string, f os.FileInfo, err error) error {
		if f == nil || f.IsDir() {
			return err
		}
		pre := filepath.Base(path)
		if strings.HasPrefix(pre, t.Uuid) {
			_ = os.Remove(path)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	file := c.TaskErrorPath + t.Uuid + ".log"
	if ok, _ := helper.PathExists(file); ok {

		if err != nil {
			return false, err
		}
	}

	// 移除任务文件
	file = c.TaskPath + t.Uuid
	if ok, _ := helper.PathExists(file); ok {
		err := os.Remove(file)
		if err != nil {
			return false, err
		}
	}

	// 移除resume文件
	t.RemoveResumeFile()
	return true, nil
}

// TaskDetail 获取任务状态
func (_this *Loop) TaskDetail(uuid string) *Task {
	t, ok := _this.tasks[uuid]
	if ok {
		return t
	}
	return nil
}

// TaskList 任务状态
func (_this *Loop) TaskList() map[string]*Task {
	return _this.tasks
}