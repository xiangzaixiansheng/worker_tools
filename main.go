package main

import (
	"os"
	config_util "worker_tools/config"
	"worker_tools/node"
)

func main() {
	//init config
	config_util.InitConfig()

	nodeType := os.Args[1]
	switch nodeType {
	case "master":
		node.GetMasterNode().Start()
	case "worker":
		node.GetWorkerNode().Start()
	default:
		panic("invalid node type")
	}
}
