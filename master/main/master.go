package main

import (
	"crotab/project/crontab/master"
	"flag"
	"fmt"
	"runtime"
	"time"
)

//设置内核数
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var (
	confFile string //配置文件路径
)

//解析命令行参数
func initArgs() {
	//master -config ./master/json
	//master -h 查看帮助
	flag.StringVar(&confFile, "config", "./master.json", "配置文件为master.json")
	//解析命令行参数
	flag.Parse()

}
func main() {
	var (
		err error
	)

	//初始化命令行参数
	initArgs()

	//初始化线程
	initEnv()

	//加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	//初始化服务发现模块
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	//日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}

	//启动任务管理器
	if err = master.InitJobMann(); err != nil {
		goto ERR
	}

	//启动api http服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	//正常退出
	for {
		time.Sleep(1 * time.Second)
	}
	return
	//异常退出
ERR:
	fmt.Println(err)
}
