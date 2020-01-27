package worker

import (
	"crotab/project/crontab/common"
	"fmt"
	"time"
)

//任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent               //etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulerPlan //任务调度计划表(记录了所有任务的下一次执行时间)
	jobExecutingTable map[string]*common.JobExecuteInfo   //执行表，保存了任务的状态
	jobResultChan     chan *common.JobExecuteResult       //任务结果队列

}

var (
	G_scheduler *Scheduler
)

//调度和执行是两件事情：调度是定期的查看哪些任务过期了；
//执行的任务可能执行很久，例如是每秒执行一次的任务，但是执行此任务需要跑一分钟。1分钟会调度60次，但是只能执行一次，防止并发
//尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulerPlan) {
	//如果任务正在执行，跳过此调度
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		//如果已经执行，这次就不需要执行了
		fmt.Println("尚未退出，跳过执行", jobPlan.Job.Name)
		return
	}
	//如果没有执行
	//构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//保存执行状态信息
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//执行任务
	fmt.Println("执行任务：", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}

//计算任务调度状态
func (scheduler *Scheduler) TryScheduler() (schedulerAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulerPlan // job + expr + nextTime
		now      time.Time
		nearTime *time.Time
	)
	//如果任务表为空，也就是没有任务需要调度
	if len(scheduler.jobPlanTable) == 0 {
		//随便一个睡眠时间
		schedulerAfter = 1 * time.Second
		return
	}

	//当前时间
	now = time.Now()

	//遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			//TODO:尝试执行任务：有可能上一次执行还没有结束，所以不一定能立即执行任务
			scheduler.TryStartJob(jobPlan)
			fmt.Println("执行任务:", jobPlan.Job.Name)
			jobPlan.NextTime = jobPlan.Expr.Next(now) //根据当前时间更新任务的下一次执行时间
		}
		//统计最近一个要过期的任务事件
		//如果nearTime是空指针 或者 当前任务的下次执行时间比nearTime要早
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			//更新nearTime
			nearTime = &jobPlan.NextTime
		}
	}
	//下次调度的时间间隔(最近要执行的任务调度时间-当前时间）
	schedulerAfter = (*nearTime).Sub(now)
	//过期的任务立即执行

	//统计最近的要过期的任务的时间（例如：N秒=schedulerAfter后过期，则睡眠N秒)
	return
}

//调度goroutine
//for检查所有的任务是否过期
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration //时间间隔
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	//初始化一次
	//第一次启动主循环之后是没有任务的，所以初始化时间为1秒
	//有任务之后则为任务的下次执行时间
	scheduleAfter = scheduler.TryScheduler()

	//延时调度
	scheduleTimer = time.NewTimer(scheduleAfter)

	//定时任务common.job
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务变化事件
			//对维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: //最近的任务到期了,对任务重新调度

		case jobResult = <-scheduler.jobResultChan: //监听任务执行结果：从结果channel中获取jobresult
			scheduler.handleJobResult(jobResult)

		}
		//调度一次任务
		scheduleAfter = scheduler.TryScheduler()
		//重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}

}

//处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulerPlan
		err             error
		jobExisted      bool
		jobExecuteInfo  *common.JobExecuteInfo
		isJobExecuting  bool
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: //保存任务事件
		if jobSchedulePlan, err = common.BuildJobSchedulrPlan(jobEvent.Job); err != nil {
			//解析失败,忽略这个任务
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan

	case common.JOB_EVENT_DELETE: //删除任务事件
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			//如果存在，则删掉
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILLE: //强杀任务事件
		//取消掉commond执行
		//判断任务是否在执行
		if jobExecuteInfo, isJobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; isJobExecuting {
			//通过cancelFunc终止command执行
			jobExecuteInfo.CancleFunc()
		}

	}
}

//处理执行结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	//从执行表中删除
	//删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	//生成执行日志
	//当错误原因不是锁被占用的时候的日志才会被记录
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			OutPut:       string(result.OutPut),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		//todo:将日志存储的
		//插入需要耗时，会影响任务的调度，如果调度比较慢的话，会使调度失去精度，甚至终止了调度
		//所以将日志转发的单独的处理模块(另一个goroutine)中执行
		G_logSink.Append(jobLog)
	}
}

//向scheduler中推送任务变化的事件
func (schedule *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	schedule.jobEventChan <- jobEvent
}

//初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulerPlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),   //执行状态表
		jobResultChan:     make(chan *common.JobExecuteResult, 1000), //任务结果队列
	}
	//启动调度goroutine
	go G_scheduler.scheduleLoop()

	return
}

//回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
