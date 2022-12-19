package main

import (
	"os"
	"worker_tools/node"
)

func main() {
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
