package master

import (
	"context"
	"crotab/project/crontab/common"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"time"
)

//任务滚利器
type JobMannger struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	//单例
	G_jobMannger *JobMannger
)

//初始化管理器
func InitJobMann() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     //集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, //连接超时时间

	}

	//建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	//获得KV和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	//赋值单例
	G_jobMannger = &JobMannger{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

//将任务保存到etcd中
func (jobmannger *JobMannger) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)
	//在etcd中保存的key
	jobKey = common.JOB_DIR + job.Name
	//在etcd中保存的value：json格式的信息
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	//将key和value存储到etcd中,并返回旧值
	if putResp, err = jobmannger.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}

	//如果是更新，就返回旧的值
	if putResp.PrevKv != nil {
		//对获取到的旧的值反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

//删除任务
func (jobmannger *JobMannger) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		deleResp  *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	//保存在etcd任务中的key
	jobKey = common.JOB_DIR + name

	//从etcd服务中删除任务
	if deleResp, err = jobmannger.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	//返回被删除的信息
	if len(deleResp.PrevKvs) != 0 {
		//解析旧值
		if err = json.Unmarshal(deleResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return oldJob, err
}

//任务list
func (jobmannger *JobMannger) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)
	//任务目录
	dirKey = common.JOB_DIR

	//获取目录下的所有任务
	if getResp, err = jobmannger.kv.Get(context.TODO(), dirKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	//初始化数组空间
	jobList = make([]*common.Job, 0)

	//遍历所有任务，进行反序列化
	for _, kvPair = range getResp.Kvs {
		fmt.Println(kvPair.Value)
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobmannger *JobMannger) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名
	var (
		killerKey      string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
	)

	// 通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	if leaseGrantResp, err = jobmannger.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobmannger.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}
	return
}
