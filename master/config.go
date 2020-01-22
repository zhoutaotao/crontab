package master

import (
	"encoding/json"
	"io/ioutil"
)

//master配置
type Config struct {
	ApiPort         int `json:"apiPort"`
	ApiReadTimeout  int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
}

var (
	//单例
	G_config *Config
)

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	//读取配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//将json反序列化到config对象中
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	//赋值单例
	G_config = &conf

	return
}