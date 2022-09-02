package loop

import (
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/log"
	"fmt"
	"time"
)

// TaskStatistic 用于任务统计
type TaskStatistic struct {
	TaskId              string    // 创建时间
	CreatedAt           time.Time // 创建时间
	PreSecCount         int32     // 每秒数据量
	FullReadCount       int64     // 数据源读取量
	PreCount            int64     // 每unit大概
	PreTime             time.Time // 每unit时间
	PreFailedCount      int64     // 失败数据,每unit大概
	PreFailedTime       time.Time // 失败数据,每unit时间
	FailedDeliveryCount int64     // 错误数量
	FailedPipeLineCount int64     // pipeline处理失败的数据数据量
	LatestRunStartAt    time.Time // 最近一次运行开始时间
	LatestRunEndAt      time.Time // 最近一次运行结束时间
	exitC               chan bool // 退出
}

func NewTaskStatistic(taskId string) *TaskStatistic {
	t := new(TaskStatistic)
	t.TaskId = taskId
	t.CreatedAt = time.Now()
	t.exitC = make(chan bool)
	return t
}

// Run 采集数据
func (s *TaskStatistic) Run(sec uint8) {
	log.Info("任务:" + s.TaskId + "的统计开始运行")
	s.LatestRunStartAt = time.Now()
	go func() {
		tk1 := time.NewTicker(time.Duration(773) * time.Millisecond) // 采集读取数据,使用质数
		tk2 := time.NewTicker(time.Duration(sec*60) * time.Second)   // 用于收集错误
		defer func() {
			log.Info("任务:" + s.TaskId + " 的统计运行结束")
			tk1.Stop()
			tk2.Stop()
		}()
		for {
			select {
			case <-s.exitC:
				return
			case <-tk1.C:
				s.PreCount = s.FullReadCount
				s.PreTime = time.Now()
			case <-tk2.C:
				s.PreFailedCount = s.FailedDeliveryCount
				s.PreFailedTime = time.Now()
			}
		}
	}()
}

// Stop 停止采集
func (s *TaskStatistic) Stop() {
	s.exitC <- true
	s.LatestRunEndAt = time.Now()
}

// UnitFailed 单位时间错误
func (s *TaskStatistic) UnitFailed() bool {
	return (s.FailedDeliveryCount - s.PreFailedCount) > 0
}

// Display 显示信息
func (s *TaskStatistic) Display(taskState int) map[string]interface{} {

	latestRunStartAt := s.LatestRunStartAt.Unix()
	if latestRunStartAt < 0 {
		latestRunStartAt = 0
	}
	latestRunEndAt := s.LatestRunEndAt.Unix()
	if latestRunEndAt < 0 {
		latestRunEndAt = 0
	}
	// 每秒多少条数据
	var PreSec uint32
	if taskState == cst.TaskRun && latestRunStartAt > 0 {
		sec := time.Now().Unix() - s.PreTime.Unix()
		if sec > 0 {
			cnt := s.FullReadCount - s.PreCount
			PreSec = uint32(cnt / sec)
		}
	}

	latestRunDuration := "0s"
	if latestRunStartAt > 0 && latestRunEndAt > 0 {
		latestRunDuration = s.LatestRunEndAt.Sub(s.LatestRunStartAt).String()
	}
	if latestRunStartAt > 0 && latestRunEndAt == 0 {
		latestRunDuration = time.Now().Sub(s.LatestRunStartAt).String()
	}
	f := s.FailedDeliveryCount + s.FailedPipeLineCount
	return map[string]interface{}{
		"FullReadCount":       s.FullReadCount,
		"FullDeliveryCount":   s.FullReadCount - f,
		"FailedDeliveryCount": s.FailedDeliveryCount,
		"FailedPipeLineCount": s.FailedPipeLineCount,
		"LatestRunStartAt":    latestRunStartAt,
		"PreSec":              fmt.Sprintf("%d/s", PreSec),
		"LatestRunDuration":   latestRunDuration,
		"LatestRunEndAt":      latestRunEndAt,
	}
}
