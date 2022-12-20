package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func Test_redis(t *testing.T) {
	err := NewDriver(&Conf{
		Host: "127.0.0.1",
		Port: 6379,
	}, redis.DialConnectTimeout(time.Second*10))
	if err != nil {
		return
	}
	rd := GetRedisInstance()
	//设置超时时间
	rd.SetTimeout(20 * time.Second)
	//注册worker
	worker_node_key, err := rd.RegisterWorkereNode("worker_node")
	fmt.Println("worker_node_key", worker_node_key)
	//注册心跳
	rd.SetHeartBeat(worker_node_key)

	time.Sleep(2 * time.Minute)

}
