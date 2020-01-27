package master

import (
	"context"
	"crotab/project/crontab/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"time"
)

//MongoDB日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)
	//建立MongoDB连接
	if client, err = mongo.Connect(context.TODO(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MgConnectTimeOut)*time.Millisecond)); err != nil {
		return
	}
	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

//查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  mongo.Cursor
		jobLog  *common.JobLog
	)

	//当LogArr为空的时候
	logArr = make([]*common.JobLog, 0)
	//过滤条件
	filter = &common.JobLogFilter{JobName: name}

	//按照任务开始时间倒序排列
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	//
	if cursor, err = logMgr.logCollection.Find(context.TODO(),
		filter, findopt.Sort(logSort),
		findopt.Skip(int64(skip)),
		findopt.Limit(int64(limit))); err != nil {
		return
	}
	//释放游标
	defer cursor.Close(context.TODO())

	//遍历游标
	for cursor.Next(context.TODO()) {
		//创建内存空间
		jobLog = &common.JobLog{}
		//反序列化bson 到jobLog中
		if err = cursor.Decode(jobLog); err != nil {
			continue
		}
		logArr = append(logArr, jobLog)

	}
	return
}
