package worker

import (
	"crotab/project/crontab/common"
	"math/rand"
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
			cmd     *exec.Cmd
			err     error
			outPut  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)
		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			OutPut:      make([]byte, 0),
		}

		//初始化锁
		jobLock = G_jobMannger.CreateJobLock(info.Job.Name)

		//记录任务的开始时间
		result.StartTime = time.Now()

		//在抢锁之前随机睡眠(0-1秒)不同的节点的时间误差在一秒之内
		//随机之后竞争就相对均匀
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		//尝试上锁
		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil { //上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else { //上锁成功，执行shell命令
			//上锁成功后，重置任务启动时间
			result.StartTime = time.Now()

			//执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			//执行并捕获输出
			outPut, err = cmd.CombinedOutput()

			//记录任务--结束时间
			result.EndTime = time.Now()

			result.OutPut = outPut
			result.Err = err

			//scheduler会从executingTable中删除此条执行记录

			//任务执行完之后，释放锁
			//jobLock.Unlock()
		}

		//任务执行完成后,把执行的结果返回给scheduler
		G_scheduler.PushJobResult(result)

	}()
}

//初始化执行器
func InitExcutor() (err error) {
	G_executor = &Executor{}
	return
}
