package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"time"

	redis_driver "worker_tools/driver"

	"github.com/gomodule/redigo/redis"

	"google.golang.org/grpc"
)

type WorkerNode struct {
	conn *grpc.ClientConn  // grpc client connection
	c    NodeServiceClient // grpc client
}

func (n *WorkerNode) Init() (err error) {
	//redis
	err = redis_driver.NewDriver(&redis_driver.Conf{
		Host: "127.0.0.1",
		Port: 6379,
	}, redis.DialConnectTimeout(time.Second*10))
	if err != nil {
		return
	}

	// connect to master node
	n.conn, err = grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		return err
	}

	// grpc client
	n.c = NewNodeServiceClient(n.conn)

	//register worker
	registerWorker()
	return nil
}

func (n *WorkerNode) Start() {
	fmt.Println("worker node started")

	// report status
	_, _ = n.c.ReportStatus(context.Background(), &Request{})

	// assign task
	stream, _ := n.c.AssignTask(context.Background(), &Request{})
	for {
		// receive command from master node
		res, err := stream.Recv()
		if err != nil || res.Data == "" {
			return
		}

		strCommand := res.Data
		fmt.Println("strCommand: ", strCommand)

		// execute command
		cmd := exec.Command("/bin/bash", "-c", strCommand)
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			fmt.Println("Execute failed when Start:" + err.Error())
			return
		}
		out_bytes, _ := ioutil.ReadAll(stdout)
		stdout.Close()
		if err := cmd.Wait(); err != nil {
			fmt.Println("Execute failed when Wait:" + err.Error())
			return
		}
		fmt.Println(strings.TrimSpace(string(out_bytes)))
	}
}

func registerWorker() (err error) {
	rd := redis_driver.GetRedisInstance()
	//设置超时时间
	rd.SetTimeout(20 * time.Second)
	//注册worker
	worker_node_key, err := rd.RegisterWorkereNode("worker_node")
	fmt.Println("worker_node_key", worker_node_key)
	//注册心跳
	rd.SetHeartBeat(worker_node_key)
	return
}

var workerNode *WorkerNode

func GetWorkerNode() *WorkerNode {
	if workerNode == nil {
		// node
		workerNode = &WorkerNode{}

		// initialize node
		if err := workerNode.Init(); err != nil {
			panic(err)
		}
	}

	return workerNode
}
