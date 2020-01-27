package worker

import (
	"context"
	"crotab/project/crontab/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"time"
)

//使用MongoDB存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	//单例
	G_logSink *LogSink
)

//批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
	//此处不判断成功与否
}

//日志存储goroutine
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch //当前的批次
		commitTimer  *time.Timer      //自动提交
		timeoutBatch *common.LogBatch //超时处理

	)
	for {
		select {
		case log = <-logSink.logChan: //从log 的channel中获取日志
			//如果每次插入一条，会因为网络往返耗时，增加总体的耗时
			//所以暂存多条日志后统一存储
			if logBatch == nil { //logBatch为空指针，也就是log是第一条日志
				logBatch = &common.LogBatch{}
				//通过时间限制提交日志
				commitTimer = time.AfterFunc(time.Duration(G_config.JobCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						//此回调函数会在另一个goroutine中执行
						//并发超时通知，不要直接提交batch
						return func() {
							//时间到，向channel中发送batch
							logSink.autoCommitChan <- batch
						}
						//logBatch是指针，在后边会被修改，所有使用参数传递的方式
					}(logBatch),
				)
			}

			//通过条数限制提交日志

			//把新的日志追加到批次数组中
			logBatch.Logs = append(logBatch.Logs, log)

			//如果批次满了，立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				//执行保存日志，可以将此操作放入一个goroutine中
				logSink.saveLogs(logBatch)
				//清空logbatch
				logBatch = nil
				//取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan: //超时需要处理的batch写入MongoDB
			if timeoutBatch != logBatch {
				continue //跳过已经被提交的批次s
			}
			logSink.saveLogs(timeoutBatch)
			logBatch = nil //将当前的批次设置为空

		}
	}
}

//发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog: //logChan满了之后，此处就会阻塞
	default:
		//队列满了就丢弃
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)
	//建立MongoDB连接
	if client, err = mongo.Connect(
		context.TODO(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MgConnectTimeOut)*time.Millisecond)); err != nil {
		return
	}
	//选择DB和Collection
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000), //可以在配置文件中配置队列的长度
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	//在goroutine中写入MongoDB
	go G_logSink.writeLoop()

	return
}
