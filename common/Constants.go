package common

const (
	//任务目录
	JOB_DIR = "/cron/jobs/"

	//任务强杀目录
	JOB_KILLER_DIR = "/cron/killer"

	//事件类型--保存
	JOB_EVENT_SAVE = 1

	//事件类型--删除
	JOB_EVENT_DELETE = 2

	//事件类型-强杀
	JOB_EVENT_KILLE = 3

	//抢锁路径 任务锁目录
	JOB_LOCK_DIR = "/cron/lock/"

	//服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"
)
