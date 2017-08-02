package config

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

// ExchangeBinding describes details about an exchanges bindings
type ExchangeBinding struct {
	Routing  string
	Exchange string
}

// Exchange describes details about a specific exchange
type Exchange struct {
	Type       string
	Durable    bool
	Autodelete bool
	Bindings   []ExchangeBinding `json:"bind"`
	Default    bool

	name string
	ch   *amqp.Channel
}

// Publish will publish to the exchange
func (e *Exchange) Publish(key string, msg amqp.Publishing) error {
	return e.ch.Publish(e.name, key, false, false, msg)
}

// Name returns the name of the exchange
func (e *Exchange) Name() string {
	return e.name
}

// Job describes a queue job
type Job struct {
	Queue   string
	Publish string
	Worker  string
}

// Queue is the details around a specific queue
type Queue struct {
	Exchange   string
	Private    bool
	Durable    bool
	Autodelete bool
	Routing    string

	name string
	ch   *amqp.Channel
}

// Name returns the queue name
func (q *Queue) Name() string {
	return q.name
}

// Consume will setup a channel for receiving queue messages
func (q *Queue) Consume(autoAck bool, exclusive bool, noLocal bool) (string, <-chan amqp.Delivery, error) {
	consumerTag := "consumer-" + randString(15)
	d, err := q.ch.Consume(q.name, consumerTag, autoAck, exclusive, noLocal, false, nil)
	return consumerTag, d, err
}

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// Cancel a consumer
func (q *Queue) Cancel(consumerTag string) error {
	return q.ch.Cancel(consumerTag, false)
}

// Config describes a configuration
type Config struct {
	URL       string `json:"url"`
	Exchanges map[string]*Exchange
	Queues    map[string]*Queue
	Jobs      map[string]*Job

	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange *Exchange
}

// Publish will publish a message to the default queue
func (c *Config) Publish(key string, msg amqp.Publishing) error {
	return c.exchange.Publish(key, msg)
}

// Connect will connect to the MQ server and wire up all the resources based on the config
func (c *Config) Connect() error {
	conn, err := amqp.Dial(c.URL)
	if err != nil {
		return fmt.Errorf("Error connecting to %s. %v", c.URL, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Error creating channel to %s. %v", c.URL, err)
	}

	c.conn = conn
	c.ch = ch

	// create all our exchanges
	for name, ex := range c.Exchanges {
		if err := ch.ExchangeDeclare(name, ex.Type, ex.Durable, ex.Autodelete, false, false, nil); err != nil {
			return fmt.Errorf("error creating exchange named `%s`. %v", name, err)
		}
		ex.name = name
		ex.ch = ch
		if c.exchange == nil && ex.Default {
			c.exchange = ex
		}
	}

	// now bind the exchanges
	for name, ex := range c.Exchanges {
		if len(ex.Bindings) > 0 {
			for _, binding := range ex.Bindings {
				// fmt.Printf("binding %s -> %s via %s\n", binding.Exchange, name, binding.Routing)
				if err := ch.ExchangeBind(name, binding.Routing, binding.Exchange, false, nil); err != nil {
					return fmt.Errorf("error binding exchange named `%s` to `%s`. %v", name, binding.Exchange, err)
				}
			}
		}
	}

	// now create queues
	for name, queue := range c.Queues {
		if queue.Private {
			q, err := ch.QueueDeclare("", queue.Durable, queue.Autodelete, true, false, nil)
			if err != nil {
				return fmt.Errorf("error creating private queue named `%s`. %s", name, err)
			}
			queue.name = q.Name
		} else {
			_, err := ch.QueueDeclare(name, queue.Durable, queue.Autodelete, false, false, nil)
			if err != nil {
				return fmt.Errorf("error creating queue named `%s`. %s", name, err)
			}
			queue.name = name
		}
		if err := ch.QueueBind(queue.name, queue.Routing, queue.Exchange, false, nil); err != nil {
			return fmt.Errorf("error binding queue named `%s` to exchange named `%s`. %s", name, queue.Exchange, err)
		}
		queue.ch = ch
	}

	return nil
}

// Close the configuration and release any opened resources
func (c *Config) Close() error {
	if c.ch != nil {
		if err := c.ch.Close(); err != nil {
			return err
		}
		c.ch = nil
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}

// New will load a config from the reader and return it
func New(r io.Reader) (*Config, error) {
	dec := json.NewDecoder(r)
	config := &Config{}
	if err := dec.Decode(config); err != nil {
		return nil, err
	}
	if config.URL == "" {
		config.URL = "amqp://guest:guest@localhost:5672/"
	}
	return config, nil
}
