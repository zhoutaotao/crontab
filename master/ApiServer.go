package master

import (
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
func handleJobSave(w http.ResponseWriter, r *http.Request) {

}

//初始化服务
func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)

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
