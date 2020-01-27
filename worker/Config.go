package worker

import (
	"encoding/json"
	"io/ioutil"
)

var (
	//单例
	G_config *Config
)

//worker配置
type Config struct {
	EtcdEndpoints    []string `json:"etcdEndpoints"`
	EtcdDialTimeout  int      `json:"etcdDialTimeout"`
	MongodbUri       string   `json:"mongodbUri"`
	MgConnectTimeOut int      `json:"mgConnectTimeOut"`
	JobLogBatchSize  int      `json:"jobLogBatchSize"`
	JobCommitTimeout int      `json:"jobCommitTimeout"`
}

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	//读取配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//将配置文件中的json反序列化到config对象中
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	//赋值单例
	G_config = &conf

	return
}
