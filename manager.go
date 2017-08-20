package jupiter

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

var jobCounter int32

type jobRouter struct {
	id          int32
	job         *Job
	config      *Config
	queue       *Queue
	consumerTag string
	publish     string
	worker      Worker
	errors      chan<- error
}

func (r *jobRouter) close() {
	r.queue.Close()
}

func (r *jobRouter) run() {
	for msg := range r.queue.session.ConsumerChannel() {
		var mqout, redisout bytes.Buffer
		in := bytes.NewReader(msg.Body)
		work := WorkMessage{r.config, msg.Type, msg.AppId, msg.MessageId, msg.CorrelationId, msg.ContentType, msg.ContentEncoding, in, &mqout, &redisout, r.job}
		wg := sync.WaitGroup{}
		wg.Add(1)
		done := func(err error) {
			wg.Done()
			if err != nil {
				r.errors <- err
				return
			}
			if mqout.Len() > 0 || redisout.Len() > 0 {
				exp, err := r.job.Expires()
				if err != nil {
					r.errors <- fmt.Errorf("error parsing job expiration `%s`. %v", r.job.Expiration, err)
					return
				}
				key := work.ResultKey()
				if mqout.Len() > 0 && r.job.DestinationMQ() {
					if err := r.config.Publish(r.publish, mqout.Bytes(), WithCorrelation(msg.MessageId), WithAppID(key), WithExpiration(exp)); err != nil {
						r.errors <- fmt.Errorf("error sending message. %v", err)
						return
					}
				}
				if redisout.Len() > 0 && r.job.DestinationRedis() {
					if err := r.config.RedisClient().Set(key, redisout.Bytes(), exp).Err(); err != nil {
						r.errors <- fmt.Errorf("error setting job result to redis. %v", err)
						return
					}
				}
			}
		}
		if err := r.worker.Work(work, done); err != nil {
			r.errors <- fmt.Errorf("error processing work. %v", err)
			msg.Nack(false, true)
			return
		}
		// wait to receive our ack on our channel and ack on
		// the same goroutine that received the message
		wg.Wait()
		msg.Ack()
	}
}

// WorkerManager is an implementation of a Manager
type WorkerManager struct {
	workers map[string]Worker
	config  *Config
	routers []*jobRouter
	errors  chan error
}

// ErrorChannel will return a channel for receiving errors
func (m *WorkerManager) ErrorChannel() <-chan error {
	return m.errors
}

// NewManager will return a new WorkerManager based on Config
func NewManager(config *Config) (*WorkerManager, error) {
	manager := &WorkerManager{
		workers: make(map[string]Worker),
		config:  config,
		routers: make([]*jobRouter, 0),
		errors:  make(chan error, 10),
	}
	ctx := context.Background()
	if err := autoregister(manager); err != nil {
		return nil, err
	}
	if err := config.Setup(ctx); err != nil {
		return nil, err
	}
	var defaultCount int
	if config.Channel.PrefetchCount == nil || *config.Channel.PrefetchCount == 0 {
		defaultCount = 1
	} else {
		defaultCount = *config.Channel.PrefetchCount
	}
	for name, job := range config.Jobs {
		q := config.Queues[job.Queue]
		if q == nil {
			return nil, fmt.Errorf("error creating job named `%s`. queue named `%s` not found", name, job.Queue)
		}
		if job.Worker == "" {
			return nil, fmt.Errorf("error creating job named `%s`. no `worker` configuration", name)
		}
		worker := manager.workers[job.Worker]
		if worker == nil {
			return nil, fmt.Errorf("error creating job named `%s`. worker named `%s` not found", name, job.Worker)
		}
		q.name = name
		q.config = config
		q.session = config.Exchanges[q.Exchange].session
		count := job.Concurrency
		if count == 0 {
			count = defaultCount
		}
		// run N number of goroutines that match the pre-fetch count so that
		// we will process at the same concurrency as pre-fetch
		for i := 0; i < count; i++ {
			j := &jobRouter{
				id:      atomic.AddInt32(&jobCounter, 1),
				job:     job,
				config:  config,
				queue:   q,
				worker:  worker,
				publish: job.Publish,
				errors:  manager.errors,
			}
			manager.routers = append(manager.routers, j)
			go j.run()
		}
		q.session.StartConsumer(q.Exchange, name, q.Routing)
	}
	return manager, nil
}

// Close will shutdown the manager and stop all job processing
func (m *WorkerManager) Close() {
	for _, job := range m.routers {
		job.close()
	}
}

// Register will register a worker by name
func (m *WorkerManager) Register(name string, worker Worker) error {
	if m.workers[name] != nil {
		return fmt.Errorf("worker named `%s` already registered", name)
	}
	m.workers[name] = worker
	return nil
}
