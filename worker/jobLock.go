package worker

import (
	"context"
	"crotab/project/crontab/common"
	"github.com/coreos/etcd/clientv3"
)

//通过TXN事务来抢锁,并通过lease去自动过期,避免宕机后锁一直占用
type JobLock struct {
	kv    clientv3.KV
	lease clientv3.Lease

	jobName    string             //任务名
	cancelFunc context.CancelFunc //用于终止自动续租
	leaseId    clientv3.LeaseID   //租约id
	isLocked   bool               //是否上锁成功
}

//初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
	return
}

//尝试上锁：抢占乐观锁，能抢到就用，抢不到就放弃
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrangeResp *clientv3.LeaseGrantResponse
		cancelCtx       context.Context
		cancelFunc      context.CancelFunc
		leaseId         clientv3.LeaseID
		keepRespChan    <-chan *clientv3.LeaseKeepAliveResponse
		txn             clientv3.Txn
		lockKey         string
		txnResp         *clientv3.TxnResponse
	)
	//创建租约 5秒
	if leaseGrangeResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	//context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	//获得租约ID
	leaseId = leaseGrangeResp.ID

	//自动续租 如果上锁成功需要一直续租，不要让租约过期
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		//续租失败
		goto FAIL
	}

	//处理续租应答的goroutine
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepRespChan: //从channel中取出续租的应答
				if keepResp == nil { //空指针时说明自动续租被取消掉了
					goto END
				}
			}
		}
	END:
		//退出goroutine
	}()

	//创建事务
	txn = jobLock.kv.Txn(context.TODO())

	//锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	//事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))). //不存在的时候添加
		Else(clientv3.OpGet(lockKey))                                   //已经存在则获取
	//提交事务
	if txnResp, err = txn.Commit(); err != nil {
		//提交事务失败
		//有可能已经抢到锁，但是通过网络应答时失败
		goto FAIL
	}

	//成功返回
	if !txnResp.Succeeded { //锁已经被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}
	//抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return

FAIL:
	//失败释放租约
	cancelFunc() //取消自动续租
	jobLock.lease.Revoke(context.TODO(), leaseId) //释放租约
	return
}

//释放锁
func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()                                  //取消自动续租的携程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId) //释放租约
	}

}
