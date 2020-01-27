package master

import (
	"crotab/project/crontab/common"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"
)

//任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	//单利对象
	G_apiServer *ApiServer
)

//保存任务的结构
//POST job = {"name:""job1","command":"echo hello","cronExpr:"*******""}
func handleJobSave(resp http.ResponseWriter, r *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	//1保存任务到etcd中
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//2读取表单中的job字段
	postJob = r.PostForm.Get("job")

	//3反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	//4将反序列化后的job，保存在etcd中，etcd是由jobMannger维护的,所以传给jobMannger
	if oldJob, err = G_jobMannger.SaveJob(&job); err != nil {
		goto ERR
	}
	//5 无错误，正常应答 {"error":0,"msg":"","data":{....}}
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		//将old返回
		resp.Write(bytes)
	}
	return
ERR:

	//6有错误的应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//删除任务
func handleJobDelete(resp http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)
	//解析form
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	//获得任务名称
	name = r.PostForm.Get("name")

	//删除任务
	if oldJob, err = G_jobMannger.DeleteJob(name); err != nil {
		goto ERR
	}
	//正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	//异常应答
	if bytes, err = common.BuildResponse(0, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//任务列表
func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)
	if jobList, err = G_jobMannger.ListJobs(); err != nil {
		goto ERR
	}

	//正常应答
	if bytes, err = common.BuildResponse(0, "list-success", jobList); err == nil {
		w.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(0, "list-error", jobList); err == nil {
		w.Write(bytes)
	}

}

// 强制杀死某个任务
// POST /job/kill  name=job1
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err   error
		name  string
		bytes []byte
	)

	// 解析POST表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 要杀死的任务名
	name = req.PostForm.Get("name")

	// 杀死任务
	if err = G_jobMannger.KillJob(name); err != nil {
		goto ERR
	}

	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	var (
		err        error
		name       string //任务名字
		skipParam  string //从第几条开始
		limitParam string //返回的条数
		skip       int
		limit      int
		logArr     []*common.JobLog
		bytes      []byte
	)
	//解析GET参数
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	//获取请求参数 /job/log?name=job10&skip=0&limit=10
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")

	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 10
	}

	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	//异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}

}

//服务发现接口
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)
	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}
	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	//异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

//初始化服务
func InitApiServer() (err error) {
	var (
		mux           *http.ServeMux
		listener      net.Listener
		httpServer    *http.Server
		staticDir     http.Dir     //静态文件的根目录
		staticHandler http.Handler //静态文件的回调handler
	)
	mux = http.NewServeMux()
	//保存
	mux.HandleFunc("/job/save", handleJobSave)
	//删除
	mux.HandleFunc("/job/delete", handleJobDelete)
	//列表
	mux.HandleFunc("/job/list", handleJobList)
	//强杀任务
	mux.HandleFunc("/job/kill", handleJobKill)
	//日志
	mux.HandleFunc("/job/log", handleJobLog)
	//服务发现
	mux.HandleFunc("/worker/list", handleWorkerList)

	//当请求的接口是以上则执行，如果不在上边就查找静态文件
	//静态文件目录

	staticDir = http.Dir("./webroot")

	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler)) //将/过滤掉之后交给staticHandler处理

	//启动tcp监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}
	//创建一个http服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,  //读超时 毫秒
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond, //写超时 5秒
		Handler:      mux,                                                        //路由
	}
	//赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}
	//启动服务端
	go httpServer.Serve(listener)
	return

}
