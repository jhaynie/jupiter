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
		done := func(err error) {
			defer msg.Ack(false)
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
				key := "jupiter." + msg.MessageId + "." + hashStrings(r.job.Name()) + ".result"
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
		in := bytes.NewReader(msg.Body)
		if err := r.worker.Work(WorkMessage{r.config, msg.Type, msg.AppId, msg.MessageId, msg.CorrelationId, msg.ContentType, msg.ContentEncoding, in, &mqout, &redisout}, done); err != nil {
			log.Println("error processing work", err)
			return
		}
	}
}

// WorkerManager is an implementation of a Manager
type WorkerManager struct {
	workers map[string]Worker
	config  *Config
	routers map[string]*jobRouter
}

// NewManager will return a new WorkerManager based on Config
func NewManager(config *Config) (*WorkerManager, error) {
	manager := &WorkerManager{
		workers: make(map[string]Worker),
		config:  config,
		routers: make(map[string]*jobRouter),
	}
	if err := autoregister(manager); err != nil {
		return nil, err
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
		go j.run()
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
