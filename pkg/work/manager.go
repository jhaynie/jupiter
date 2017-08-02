package work

import (
	"bytes"
	"fmt"
	"log"

	"github.com/jhaynie/jupiter/pkg/config"
	"github.com/jhaynie/jupiter/pkg/types"
	"github.com/streadway/amqp"
)

type jobRouter struct {
	config      *config.Config
	queue       *config.Queue
	consumerTag string
	publish     string
	ch          <-chan amqp.Delivery
	worker      types.Worker
}

func (r *jobRouter) close() {
	if r.consumerTag != "" {
		r.queue.Cancel(r.consumerTag)
		r.consumerTag = ""
	}
}

func (r *jobRouter) run() {
	for msg := range r.ch {
		var out bytes.Buffer
		done := func(err error) {
			defer msg.Ack(false)
			if err != nil {
				log.Println("error processing work", err)
				return
			}
			if r.publish != "" && out.Len() > 0 {
				outmsg := amqp.Publishing{
					Type: r.publish,
					Body: out.Bytes(),
				}
				if err := r.config.Publish(r.publish, outmsg); err != nil {
					log.Println("error sending message", err)
					return
				}
			}
		}
		in := bytes.NewReader(msg.Body)
		if err := r.worker.Work(in, &out, done); err != nil {
			log.Println("error processing work", err)
			return
		}
	}
}

// WorkerManager is an implementation of a Manager
type WorkerManager struct {
	workers map[string]types.Worker
	config  *config.Config
	routers map[string]*jobRouter
}

// New will return a new WorkerManager based on Config
func New(config *config.Config) (*WorkerManager, error) {
	manager := &WorkerManager{
		workers: make(map[string]types.Worker),
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
func (m *WorkerManager) Register(name string, worker types.Worker) error {
	if m.workers[name] != nil {
		return fmt.Errorf("worker named `%s` already registered", name)
	}
	m.workers[name] = worker
	return nil
}
