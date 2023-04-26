package utils

import (
	"container/list"
	"fmt"
	"sync"
)

//Job的类
type Job struct {
	DoneChan   chan struct{}
	HandleFunc func(j *Job) error
}

func (job *Job) Done() {
	job.DoneChan <- struct{}{}
	close(job.DoneChan)
}

func (job *Job) WaitDone() {
	select {
	case <-job.DoneChan:
		return
	}
}

func (job *Job) Execute() error {
	fmt.Println("job start to execute ")
	return job.HandleFunc(job)
}

//任务队列
type JobQueue struct {
	mu sync.Mutex
	//任务关闭使用
	noticeChan chan struct{}
	//任务队列
	queue *list.List
	//任务队列实时大小
	size int
	//任务队列初始化的大小
	capacity int
}

// cap 队列的长度, 和任务通道通知的大小
func NewJobQueue(cap int) *JobQueue {
	return &JobQueue{
		capacity:   cap,
		queue:      list.New(),
		noticeChan: make(chan struct{}, cap),
	}
}

func (q *JobQueue) PushJob(job *Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.size++
	if q.size > q.capacity {
		q.RemoveLeastJob()
	}

	q.queue.PushBack(job)
	//通知有任务
	q.noticeChan <- struct{}{}
}

func (q *JobQueue) PopJob() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == 0 {
		return nil
	}

	q.size--
	return q.queue.Remove(q.queue.Front()).(*Job)
}

func (q *JobQueue) GetLength() int {
	return q.size
}

func (q *JobQueue) RemoveLeastJob() {
	if q.queue.Len() != 0 {
		back := q.queue.Back()
		abandonJob := back.Value.(*Job)
		abandonJob.Done()
		q.queue.Remove(back)
	}
}

func (q *JobQueue) waitJob() <-chan struct{} {
	return q.noticeChan
}

type WorkerManager struct {
	jobQueue *JobQueue
}

func NewWorkerManager(jobQueue *JobQueue) *WorkerManager {
	return &WorkerManager{
		jobQueue: jobQueue,
	}
}

func (m *WorkerManager) CreateWorker(workerName int) error {

	go func(index int) {
		fmt.Println("start worker success")
		var job *Job

		for {
			select {
			case <-m.jobQueue.waitJob():
				fmt.Printf("get a job from job queue No. %v worker===> \n", index)
				job = m.jobQueue.PopJob()
				fmt.Printf("start to execute job No. %v worker===> \n", index)
				job.Execute()
				fmt.Printf("execute job No. %v worker ===> done  jobqueue len (%v) \n \n \n", index, m.jobQueue.GetLength())
				job.Done()
			}
		}
	}(workerName)

	return nil
}
