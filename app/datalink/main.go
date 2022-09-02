package main

import (
	"data-link-2.0/datalink"
	"data-link-2.0/datalink/linkd"
	"data-link-2.0/datalink/loop"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/log"
	"flag"
	"os"
	"runtime"
)

const appDebug = true

// commandArgs 解析命令行
func commandArgs() map[string]interface{} {
	cmd := map[string]interface{}{}

	help := flag.Bool("help", false, "usage")
	f := flag.String("f", "", "配置文件路径")
	newTask := flag.String("new", "", "添加一个任务")
	start := flag.String("start", "", "启动一个任务")
	stop := flag.String("stop", "", "停止一个任务")
	remove := flag.String("remove", "", "移除一个任务")
	info := flag.String("info", "", "任务详情")
	list := flag.Bool("list", false, "任务列表")

	flag.Parse()

	cmd["usage"] = *help
	cmd["confFile"] = *f
	cmd["new"] = *newTask
	cmd["start"] = *start
	cmd["stop"] = *stop
	cmd["remove"] = *remove
	cmd["info"] = *info
	cmd["list"] = *list
	return cmd
}

func New(confFile string) *datalink.DataLink {

	// 加载配置文件
	c := conf.LoadConf(confFile)

	// 设置CPU运行的核数
	num1 := c.CoreNumber
	num0 := runtime.NumCPU()
	if num1 > 0 && num1 <= num0 {
		runtime.GOMAXPROCS(num1)
	}

	// 初始化 日志
	log.InitLogger(c.LogPath, appDebug)
	log.Info("数据链初始化")

	r := loop.NewLoop()
	h := linkd.Init(*c)
	server := datalink.New(r, h, *c)
	server.Debug = appDebug
	return server
}

// main 检查配置
func main() {
	cmd := commandArgs()

	// 显示帮助
	usage, ok := cmd["usage"]
	if usage.(bool) || len(os.Args) == 1 {
		flag.PrintDefaults()
		return
	}

	// 运行服务,启动一个sock文件
	confFile, ok := cmd["confFile"]
	if ok && confFile != "" {
		server := New(confFile.(string))
		server.Start()
		conf.G.AppDebug = appDebug
		server.Wait()
		return
	}

	// 帮助
	flag.PrintDefaults()
	return
}
