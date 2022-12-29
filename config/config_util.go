package config_util

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Redis  RedisConfig  `yaml:"redis-config"`
	Worker WorkerConfig `yaml:"worker-config"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Db       int    `yaml:"db"`
	Password string `yaml:"password"`
	RDBPath  string `yaml:"rdb_path"`
}

type WorkerConfig struct {
	ExecCnt int `yaml:"execCnt"`
}

var Config Configuration

func InitConfig() {
	absPath, err := filepath.Abs("./config/config.yaml")
	fmt.Println("absPath", absPath)
	if err != nil {
	}
	yamlFile, err := ioutil.ReadFile(absPath)
	if err != nil {
		return
	}
	yaml.Unmarshal(yamlFile, &Config)
	setDefaults(&Config)
}

func setDefaults(v interface{}) {
	if err := defaults.Set(v); err != nil {
		panic(err)
	}
}
