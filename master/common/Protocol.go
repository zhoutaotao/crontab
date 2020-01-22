package common

import "encoding/json"

//定时任务
type Job struct {
	Name     string `json:name`     //任务名
	Command  string `json:command`  //shell命令
	CronExpr string `json:cronexpr` //cron表达式
}

//http接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	data  interface{} `json:"data"`
}

//构建应答
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	//定义response
	var (
		response Response
	)
	//赋值
	response.Errno = errno
	response.Msg = msg
	response.data = data
	//序列化为json
	resp, err = json.Marshal(response)
	return
}
