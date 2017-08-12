package jupiter

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type jobRouter struct {
	job         *Job
	config      *Config
	queue       *Queue
	consumerTag string
	publish     string
	ch          <-chan amqp.Delivery
	worker      Worker
}

func (r *jobRouter) close() {
	if r.consumerTag != "" {
		r.queue.Cancel(r.consumerTag)
		r.consumerTag = ""
	}
}

func hashStrings(objects ...string) string {
	h := sha256.New()
	for _, o := range objects {
		io.WriteString(h, o)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (r *jobRouter) run() {
	for msg := range r.ch {
		var mqout, redisout bytes.Buffer
		in := bytes.NewReader(msg.Body)
		work := WorkMessage{r.config, msg.Type, msg.AppId, msg.MessageId, msg.CorrelationId, msg.ContentType, msg.ContentEncoding, in, &mqout, &redisout, r.job}
		ackChan := make(chan bool)
		done := func(err error) {
			defer func() { ackChan <- true }()
			if err != nil {
				log.Println("error processing work", err)
				return
			}
			if mqout.Len() > 0 || redisout.Len() > 0 {
				exp, err := r.job.Expires()
				if err != nil {
					log.Printf("error parsing job expiration `%s`. %v\n", r.job.Expiration, err)
					return
				}
				key := work.ResultKey()
				if mqout.Len() > 0 && r.job.DestinationMQ() {
					outmsg := amqp.Publishing{
						Type:          r.publish,
						Body:          mqout.Bytes(),
						CorrelationId: msg.MessageId,
						AppId:         key,
						Expiration:    fmt.Sprintf("%d", int64(time.Until(time.Now().Add(exp))/time.Millisecond)),
					}
					if err := r.config.Publish(r.publish, outmsg); err != nil {
						log.Println("error sending message.", err)
						return
					}
				}
				if redisout.Len() > 0 && r.job.DestinationRedis() {
					if err := r.config.RedisClient().Set(key, redisout.Bytes(), exp).Err(); err != nil {
						log.Println("error setting job result to redis.", err)
						return
					}
				}
			}
		}
		if err := r.worker.Work(work, done); err != nil {
			log.Println("error processing work", err)
			return
		}
		// wait to receive our ack on our channel and ack on
		// the same goroutine that received the message
		select {
		case <-ackChan:
			msg.Ack(false)
		}
	}
}

// WorkerManager is an implementation of a Manager
type WorkerManager struct {
	workers   map[string]Worker
	config    *Config
	routers   map[string]*jobRouter
	autoclose bool
}

// NewManager will return a new WorkerManager based on Config
func NewManager(config *Config) (*WorkerManager, error) {
	manager := &WorkerManager{
		workers: make(map[string]Worker),
		config:  config,
		routers: make(map[string]*jobRouter),
	}
	if !config.IsConnected() {
		if err := config.Connect(); err != nil {
			return nil, err
		}
		// if we open, we should also be responsible to close it
		manager.autoclose = true
	}
	if err := autoregister(manager); err != nil {
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
		consumerTag, ch, err := q.Consume(false, false, false)
		if err != nil {
			return nil, fmt.Errorf("error creating job named `%s`. cannot create consumer for queue named `%s`. %v", name, job.Queue, err)
		}
		j := &jobRouter{
			job:         job,
			config:      config,
			queue:       q,
			consumerTag: consumerTag,
			ch:          ch,
			worker:      worker,
			publish:     job.Publish,
		}
		manager.routers[name] = j
		count := job.Concurrency
		if count == 0 {
			count = defaultCount
		}
		// run N number of goroutines that match the pre-fetch count so that
		// we will process at the same concurrency as pre-fetch
		for i := 0; i < count; i++ {
			go j.run()
		}
	}
	return manager, nil
}

// Close will shutdown the manager and stop all job processing
func (m *WorkerManager) Close() {
	for _, job := range m.routers {
		job.close()
	}
	if m.autoclose {
		m.config.Close()
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
