package node

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

// MasterNode is the node instance
type MasterNode struct {
	api     *gin.Engine     // api server
	ln      net.Listener    // listener
	svr     *grpc.Server    // grpc server
	nodeSvr *NodeGrpcServer // node service
	wg      sync.WaitGroup  // wait group for graceful shutdown
}

func (n *MasterNode) Init() (err error) {
	// grpc server listener with port as 50051
	n.ln, err = net.Listen("tcp", ":50051")
	if err != nil {
		log.Printf("Failed to listen: %v", err)
		return err
	}

	// grpc server with options
	n.svr = grpc.NewServer(
		grpc.UnaryInterceptor(logInterceptor),
	)

	// node service
	n.nodeSvr = GetNodeGrpcServer()

	// register node service to grpc server
	RegisterNodeServiceServer(n.svr, n.nodeSvr)

	// api with recovery and logger middleware
	gin.SetMode(gin.ReleaseMode)
	n.api = gin.New()
	n.api.Use(gin.Recovery(), gin.Logger())
	
	n.api.POST("/tasks", n.handleTasks)
	return nil
}

func (n *MasterNode) handleTasks(c *gin.Context) {
	// parse payload
	var payload struct {
		Cmd string `json:"cmd" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// validate command
	if payload.Cmd == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "empty command"})
		return
	}

	// send command to worker Node by Grpc
	select {
	case n.nodeSvr.CmdChannel <- payload.Cmd:
		c.JSON(http.StatusOK, gin.H{"message": "task accepted"})
	case <-time.After(5 * time.Second):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "command channel full"})
	}
}

func (n *MasterNode) Start() {
	// setup graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// start grpc server
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		log.Printf("Starting gRPC server on :50051")
		if err := n.svr.Serve(n.ln); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// start api server
	srv := &http.Server{
		Addr:    ":3001",
		Handler: n.api,
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		log.Printf("Starting HTTP server on :3001")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// wait for shutdown signal
	<-quit
	log.Println("Shutting down servers...")

	// shutdown http server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// shutdown grpc server
	n.svr.GracefulStop()

	// wait for all goroutines to finish
	n.wg.Wait()
	log.Println("Servers shutdown complete")
}

// logInterceptor is a gRPC interceptor that logs requests
func logInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Printf("gRPC method: %s, duration: %v, error: %v", info.FullMethod, time.Since(start), err)
	return resp, err
}

var masterNode *MasterNode

// GetMasterNode returns the node instance
func GetMasterNode() *MasterNode {
	if masterNode == nil {
		masterNode = &MasterNode{}
		if err := masterNode.Init(); err != nil {
			log.Fatalf("Failed to initialize master node: %v", err)
		}
	}
	return masterNode
}
