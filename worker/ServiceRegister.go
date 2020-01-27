package worker

import (
	"context"
	"crotab/project/crontab/common"
	"github.com/coreos/etcd/clientv3"
	"net"
	"time"
)

//服务注册

//注册节点到etcd: /cron/workers/IP
type Resgister struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease

	localIp string //key为本机ip地址
}

var (
	G_register *Resgister
)

//获取本机Ip地址
func getLocallIp() (ipv4 string, err error) {
	var (
		addrs   []net.Addr
		addr    net.Addr
		ipNet   *net.IPNet //ip地址
		isIpNet bool
	)
	//获取所有网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	//获取第一个非localhost的网卡
	for _, addr = range addrs {
		//反解
		//这个网络地址是ip地址:ipv4、ipv6
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && ipNet.IP.IsLoopback() { //如果反解成功，，说明是ip地址,并且跳过换回网卡
			//跳过ipv6
			if ipNet.IP.To4() != nil { //如果不为空说明他是一个ipv4地址
				ipv4 = ipNet.IP.String() //192.168.1.1
				return
			}
		}

		err = common.ERR_NO_LOCAL_IP_FOUND
		return
	}
	return
}

//将存储的主机ip注册到etcd:/cron/workers/ip ,并自动续租
func (register *Resgister) keepOnline() {
	var (
		key            string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err            error
		keepAliveChan  <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp  *clientv3.LeaseKeepAliveResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
	)

	for {

		//注册路径
		key = common.JOB_WORKER_DIR + register.localIp

		cancelFunc = nil

		//创建租约 服务宕机10秒，租约会被删掉
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			//创建租约异常，需要重试
			goto RETRY
		}
		//获得租约成功,并为指定租约续租
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			//自动续租失败，重试
			goto RETRY
		}

		//失败时，需要取消租约
		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		//续租成功，注册到etcd
		if _, err = register.kv.Put(cancelCtx, key, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			//添加失败后重试
			goto RETRY
		}
		//处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil { //续租失败:例如因网络原因，一直没有续租上，再次连接上时，显示租约已经过期
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		//取消租约
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

//初始化服务注册
func InitRegister() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		localIp string //本机物理ip
	)
	//初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	//连接连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	//得到KV和Lease的API集合
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	//本机ip地址
	if localIp, err = getLocallIp(); err != nil {
		return
	}

	G_register = &Resgister{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIp: localIp,
	}

	//服务注册
	go G_register.keepOnline()
	return
}
