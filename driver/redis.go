package redis_driver

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

const WORDER_PREFIX_KEY = "worker-node:"

var once sync.Once

type Conf struct {
	Addr     string
	Password string

	Proto string

	Host string
	Port int

	MaxActive   int
	MaxIdle     int
	IdleTimeout time.Duration
	Wait        bool
}

type RedisDriver struct {
	conf        *Conf
	redisClient *redis.Pool
	timeout     time.Duration
}

// RedisClient Redis缓存客户端单例
var RedisClient *RedisDriver = new(RedisDriver)

// 封装 redis 实例，提供获取
func GetRedisInstance() *RedisDriver {
	return RedisClient
}

func NewDriver(conf *Conf, options ...redis.DialOption) error {

	ops := []redis.DialOption{
		redis.DialPassword(conf.Password),
	}
	ops = append(ops, options...)

	if conf.Proto == "" {
		conf.Proto = "tcp"
	}
	if conf.MaxActive == 0 {
		conf.MaxActive = 100
	}
	if conf.MaxIdle == 0 {
		conf.MaxIdle = 100
	}
	if conf.IdleTimeout == 0 {
		conf.IdleTimeout = time.Second * 5
	}
	if conf.Addr == "" {
		conf.Addr = fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	}

	rd := &redis.Pool{
		MaxIdle:     conf.MaxIdle,
		MaxActive:   conf.MaxActive,
		IdleTimeout: conf.IdleTimeout,
		Wait:        conf.Wait,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(conf.Proto, conf.Addr, ops...)
			if err != nil {
				panic(err)
			}
			return c, nil
		},
	}
	fmt.Println("redis已经连接")

	once.Do(func() {
		RedisClient = &RedisDriver{
			conf:        conf,
			redisClient: rd,
		}
	})

	return nil
}

func (rd *RedisDriver) SetTimeout(timeout time.Duration) {
	rd.timeout = timeout
}

//开启心跳
func (rd *RedisDriver) SetHeartBeat(worker_node_key string) {
	go rd.heartBeat(worker_node_key)
}

func (rd *RedisDriver) do(command string, params ...interface{}) (interface{}, error) {
	conn := rd.redisClient.Get()
	defer conn.Close()
	return conn.Do(command, params...)
}

func (rd *RedisDriver) heartBeat(worker_node_key string) {

	key := worker_node_key
	tickers := time.NewTicker(rd.timeout / 2)

	for range tickers.C {
		log.Printf("heartBear start ... %+v", key)

		keyExist, err := redis.Int(rd.do("EXPIRE", key, int(rd.timeout/time.Second)))
		if err != nil {
			log.Printf("redis expire error %+v", err)
			continue
		}
		if keyExist == 0 {
			if err := rd.registerWorkerServer(key); err != nil {
				log.Printf("register worker error %+v", err)
			}
		}
	}
}

//register worker in redis
func (rd *RedisDriver) RegisterWorkereNode(workerName string) (worker_node_key string, err error) {
	// get worker key
	worker_node_key = rd.randWorkerID(workerName)
	if err := rd.registerWorkerServer(worker_node_key); err != nil {
		return "", err
	}
	log.Printf("register worker success, worker_node_key %+v", worker_node_key)
	return worker_node_key, nil
}

func (rd *RedisDriver) GetWorkerKey(workerName string) string {
	return fmt.Sprintf("%s%s:", WORDER_PREFIX_KEY, workerName)
}

func (rd *RedisDriver) randWorkerID(workerName string) (worker_node_key string) {
	return rd.GetWorkerKey(workerName) + uuid.New().String()
}

func (rd *RedisDriver) registerWorkerServer(worker_node_key string) error {
	_, err := rd.do("SETEX", worker_node_key, int(rd.timeout/time.Second), worker_node_key)
	return err
}
