package worker

import (
	"context"
	"crotab/project/crontab/common"
	"os/exec"
	"time"
)

//任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

//执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {

	go func() {
		var (
			cmd    *exec.Cmd
			err    error
			outPut []byte
			result *common.JobExecuteResult
		)
		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			OutPut:      make([]byte, 0),
		}

		//记录任务--开始时间
		result.StartTime = time.Now()

		//执行shell命令
		cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", info.Job.Command)

		//执行并捕获输出
		outPut, err = cmd.CombinedOutput()

		//记录任务--结束时间
		result.EndTime = time.Now()

		result.OutPut = outPut
		result.Err = err

		//任务执行完成后,把执行的结果返回给scheduler
		G_scheduler.PushJobResult(result)

		//scheduler会从executingTable中删除此条执行记录

	}()
}

//初始化执行器
func InitExcutor() (err error) {
	G_executor = &Executor{}
	return
}
