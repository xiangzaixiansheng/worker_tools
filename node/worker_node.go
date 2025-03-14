package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	config_util "worker_tools/config"
	redis_driver "worker_tools/driver"
	"worker_tools/utils"

	"github.com/gomodule/redigo/redis"
	"google.golang.org/grpc"
)

type WorkerNode struct {
	conn      *grpc.ClientConn  // grpc client connection
	c         NodeServiceClient // grpc client
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type WorkerControl struct {
	jobQueue  *utils.JobQueue
	wm        *utils.WorkerManager
	closeOnce sync.Once
}

// ExecuteCommand executes a shell command and returns its output
func executeCommand(cmd string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, "/bin/bash", "-c", cmd)
	output, err := command.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("command timed out after 30 seconds")
		}
		return "", fmt.Errorf("command failed: %v", err)
	}
	return strings.TrimSpace(string(output)), nil
}

var workerControl = new(WorkerControl)

func (w *WorkerControl) CommitJob(job *utils.Job) {
	w.jobQueue.PushJob(job)
	log.Printf("Job committed successfully")
}

func (w *WorkerControl) Shutdown() {
	w.closeOnce.Do(func() {
		log.Println("Shutting down worker control...")
		// 清理所有剩余任务
		for w.jobQueue.GetLength() > 0 {
			if job := w.jobQueue.PopJob(); job != nil {
				job.Done()
			}
		}
	})
}

func (n *WorkerNode) Init() (err error) {
	// Create cancelable context
	n.ctx, n.cancel = context.WithCancel(context.Background())

	// Initialize Redis connection with retry mechanism
	redisConf := &redis_driver.Conf{
		Host: config_util.Config.Redis.Host,
		Port: config_util.Config.Redis.Port,
	}

	var redisRetryCount int
	for {
		err = redis_driver.NewDriver(redisConf, 
			redis.DialConnectTimeout(time.Second*10),
			redis.DialReadTimeout(time.Second*10),
			redis.DialWriteTimeout(time.Second*10))
		if err == nil {
			break
		}
		redisRetryCount++
		if redisRetryCount >= 5 {
			return fmt.Errorf("failed to initialize Redis after 5 attempts: %v", err)
		}
		log.Printf("Failed to connect to Redis, retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
	}

	// Connect to master node with retry mechanism
	var grpcRetryCount int
	for {
		n.conn, err = grpc.Dial("localhost:50051", 
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second))
		if err == nil {
			break
		}
		grpcRetryCount++
		if grpcRetryCount >= 5 {
			return fmt.Errorf("failed to connect to master node after 5 attempts: %v", err)
		}
		log.Printf("Failed to connect to master node, retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
	}

	// Initialize gRPC client
	n.c = NewNodeServiceClient(n.conn)

	if err = registerWorker(); err != nil {
		return fmt.Errorf("failed to register worker: %v", err)
	}

	if err = registerTaskQueue(); err != nil {
		return fmt.Errorf("failed to register task queue: %v", err)
	}

	return nil
}

func (n *WorkerNode) Start() {
	log.Println("Worker node started")
	defer n.conn.Close()

	// Setup graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Report status periodically
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ticker.C:
				if _, err := n.c.ReportStatus(n.ctx, &Request{}); err != nil {
					log.Printf("Failed to report status: %v", err)
				}
			}
		}
	}()

	// Handle tasks
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.handleTasks(); err != nil {
			log.Printf("Task handling error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-quit
	log.Println("Shutting down worker node...")
	n.cancel()
	workerControl.Shutdown()
	n.wg.Wait()
	log.Println("Worker node shutdown complete")
}

func (n *WorkerNode) handleTasks() error {
	stream, err := n.c.AssignTask(n.ctx, &Request{})
	if err != nil {
		return fmt.Errorf("failed to start task stream: %v", err)
	}

	for {
		select {
		case <-n.ctx.Done():
			return nil
		default:
			res, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("error receiving task: %v", err)
			}

			if res.Data == "" {
				continue
			}

			log.Printf("Received command: %s", res.Data)
			job := &utils.Job{
				DoneChan: make(chan struct{}, 1),
				HandleFunc: func(job *utils.Job) error {
					output, err := executeCommand(res.Data)
					if err != nil {
						log.Printf("Job execution failed: %v", err)
						return err
					}
					log.Printf("Job completed successfully. Output: %s", output)
					return nil
				},
			}

			workerControl.CommitJob(job)
		}
	}
}

func registerWorker() error {
	rd := redis_driver.GetRedisInstance()
	rd.SetTimeout(1 * time.Minute)

	workerKey, err := rd.RegisterWorkereNode("worker_node")
	if err != nil {
		return fmt.Errorf("failed to register worker node: %v", err)
	}
	log.Printf("Worker registered with key: %s", workerKey)

	// Start heartbeat
	rd.SetHeartBeat(workerKey)
	return nil
}

func registerTaskQueue() error {
	jobQueue := utils.NewJobQueue(50)
	log.Println("Job queue initialized")

	m := utils.NewWorkerManager(jobQueue)
	execCnt := config_util.Config.Worker.ExecCnt
	
	for i := 0; i < execCnt; i++ {
		if err := m.CreateWorker(i); err != nil {
			return fmt.Errorf("failed to create worker %d: %v", i, err)
		}
		log.Printf("Worker %d created", i)
	}

	workerControl = &WorkerControl{
		jobQueue: jobQueue,
		wm:       m,
	}

	return nil
}

var workerNode *WorkerNode

func GetWorkerNode() *WorkerNode {
	if workerNode == nil {
		workerNode = &WorkerNode{}
		if err := workerNode.Init(); err != nil {
			log.Fatalf("Failed to initialize worker node: %v", err)
		}
	}
	return workerNode
}
