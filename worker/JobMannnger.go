package worker

import (
	"context"
	"crotab/project/crontab/common"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

//任务滚利器
type JobMannger struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	//单例
	G_jobMannger *JobMannger
)

//建ring任务变化
func (jobMgr *JobMannger) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpair             *mvccpb.KeyValue
		job                *common.Job
		watcher            clientv3.Watcher
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)
	//获得所有任务，并且获得当前集群的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_DIR, clientv3.WithPrevKV()); err != nil {
		return
	}

	//当前有哪些任务
	for _, kvpair = range getResp.Kvs {
		//反序列化json到Job中
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			//创建保存事件
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			//把这个job同步给调度goroutine
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	//从revision向后监听变化事件
	go func() {
		//从下一个revision开始监听
		watchStartRevision = getResp.Header.Revision + 1

		//从watchStartRevision版本，监听目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())

		//处理监听事件
		//chan中存放是WatchResponse
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存事件
					//TODO：反序列化job，推一个更新事件给scheduler
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						//如果出错则忽略这个
						continue
					}
					//构建一个event事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
					//TODO:推送一个保存事件给scheduler
					fmt.Println(*jobEvent)
				case mvccpb.DELETE: //任务删除事件
					//从etcd的key中提取任务名
					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))

					job = &common.Job{
						Name: jobName,
					}

					//构建一个event事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
					//TODO:推送一个删除事件给scheduler
					fmt.Println(*jobEvent)
				}
				//将以上创建好的event推送给scheduler
				G_scheduler.PushJobEvent(jobEvent)

			}
		}

	}()
	return

}

//初始化管理器
func InitJobMann() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, //连接超时时间

	}

	//建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	//获得KV和lease、watcher的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.Watcher(client)

	//赋值单例
	G_jobMannger = &JobMannger{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	//启动任务监听
	G_jobMannger.watchJobs()
	return
}

//创建任务执行锁
func (jobMgr *JobMannger) CreateJobLock(jobName string) (jobLock *JobLock) {

	jobLock = InitJobLock(jobName, jobMgr.kv, jobMgr.lease)
	return
}
