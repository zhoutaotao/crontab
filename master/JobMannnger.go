package master

import (
	"context"
	"crotab/project/crontab/master/common"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
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
