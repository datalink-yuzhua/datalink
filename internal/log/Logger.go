package log

import (
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"
	"os"
	"runtime/debug"
	"time"
)

var logger = logrus.New()

// InitLogger 默认输出到文件
func InitLogger(dir string, debug bool) {
	// debug 时,作为标准输出
	if debug {
		logger.AddHook(&writer.Hook{
			Writer:    os.Stdout,
			LogLevels: logrus.AllLevels,
		})
	}

	// 输出格式
	logger.SetFormatter(&logrus.TextFormatter{})
	//logger.SetFormatter(&logrus.JSONFormatter{})

	// 切割文件,保留30天,每天一个新文件
	file := fmt.Sprintf("%sapp.log", dir)
	w, _ := rotatelogs.New(
		file+".%Y%m%d",
		rotatelogs.WithLinkName(file),
		rotatelogs.WithMaxAge(time.Duration(30*24)*time.Hour),
		rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
	)
	logger.SetOutput(w)
}

func Info(msg ...interface{}) {
	logger.Info(msg...)
}

func Warn(msg ...interface{}) {
	logger.Warn(msg...)
}

func Error(msg ...interface{}) {
	logger.Error(msg...)
	logger.Error(string(debug.Stack()))
}

func Debug(msg ...interface{}) {
	logger.Debug(msg...)
	logger.Debug(string(debug.Stack()))
}

func Trace(msg ...interface{}) {
	logger.Trace(msg...)
}

func Fatal(msg ...interface{}) {
	logger.Fatal(msg)
}
