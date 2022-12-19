package node

import (
	"context"
)

type NodeGrpcServer struct {
	UnimplementedNodeServiceServer

	// channel to receive command
	CmdChannel chan string
}

func (n NodeGrpcServer) ReportStatus(ctx context.Context, request *Request) (*Response, error) {
	return &Response{Data: "ok"}, nil
}

//通过channel触发传送命令
func (n NodeGrpcServer) AssignTask(request *Request, server NodeService_AssignTaskServer) error {
	for {
		select {
		case cmd := <-n.CmdChannel:
			// receive command and send to worker node (client)
			if err := server.Send(&Response{Data: cmd}); err != nil {
				return err
			}
		}
	}
}

var server *NodeGrpcServer

func GetNodeGrpcServer() *NodeGrpcServer {
	if server == nil {
		server = &NodeGrpcServer{
			CmdChannel: make(chan string),
		}
	}
	return server
}
