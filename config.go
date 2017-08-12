package jupiter

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis"
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
	if msg.MessageId == "" {
		msg.MessageId = randString(64)
	}
	return e.ch.Publish(e.name, key, false, false, msg)
}

// Name returns the name of the exchange
func (e *Exchange) Name() string {
	return e.name
}

// Job describes a queue job
type Job struct {
	Queue       string
	Publish     string
	Worker      string
	Destination string
	Expiration  string `json:"expires"`
	Concurrency int

	name string
}

// Name returns the name for a job
func (j *Job) Name() string {
	return j.name
}

// Expires will return a time.Duration when the job result expires
func (j *Job) Expires() (time.Duration, error) {
	if j.Expiration == "" {
		return time.Minute * 60 * 24, nil
	}
	return time.ParseDuration(j.Expiration)
}

// DestinationRedis will return true if the job should be sent to redis
func (j *Job) DestinationRedis() bool {
	return j.Destination == "redis" || j.Destination == "both"
}

// DestinationMQ will return true if the job should be sent to Rabbit MQ
func (j *Job) DestinationMQ() bool {
	return j.Publish != "" && (j.Destination == "mq" || j.Destination == "both" || j.Destination == "")
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

// Channel configuration detail
type Channel struct {
	PrefetchCount *int `json:"prefetch_count"`
	PrefetchSize  *int `json:"prefetch_size"`
}

// Config describes a configuration
type Config struct {
	MQURL     string `json:"mqurl"`
	RedisURL  string `json:"redisurl"`
	Channel   Channel
	Exchanges map[string]*Exchange
	Queues    map[string]*Queue
	Jobs      map[string]*Job

	conn      *amqp.Connection
	ch        *amqp.Channel
	exchange  *Exchange
	client    *redis.Client
	connected bool
	mu        sync.RWMutex
}

// IsConnected returns true if you've called Connect and not Close
func (c *Config) IsConnected() bool {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()
	return connected
}

// RedisClient will return the redis client instance
func (c *Config) RedisClient() *redis.Client {
	return c.client
}

// Publish will publish a message to the default queue
func (c *Config) Publish(key string, msg amqp.Publishing) error {
	c.mu.RLock()
	if c.exchange == nil {
		c.mu.RUnlock()
		return fmt.Errorf("no default exchange found")
	}
	err := c.exchange.Publish(key, msg)
	c.mu.RUnlock()
	return err
}

// Connect will connect to the MQ server and wire up all the resources based on the config
func (c *Config) Connect() error {
	conn, err := amqp.Dial(c.MQURL)
	if err != nil {
		return fmt.Errorf("Error connecting to %s. %v", c.MQURL, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Error creating channel to %s. %v", c.MQURL, err)
	}

	count := 1
	size := 0

	c.conn = conn
	c.ch = ch

	// run a go routine to monitor channel errors and attempt to reconnect
	go func(conn *amqp.Connection) {
		var err error
		for {
			// blocks for the channel to be closed and we
			// need to check to see if this was closed unexpectedly
			// or if we closed it on purpose
			<-conn.NotifyClose(make(chan *amqp.Error))
			c.mu.Lock()
			// only attempt to reconnect when we're not closing on purpose
			if c.connected {
				conn, err = amqp.Dial(c.MQURL)
				if err != nil {
					c.connected = false
					c.conn = nil
					c.ch = nil
					c.mu.Unlock()
					fmt.Fprintln(os.Stderr, "Couldn't reconnect to MQ server. ", err)
					return
				}
				ch, err := conn.Channel()
				if err != nil {
					c.connected = false
					c.conn.Close()
					c.conn = nil
					c.ch = nil
					c.mu.Unlock()
					fmt.Fprintln(os.Stderr, "Couldn't reconnect to MQ server and obtain channel. ", err)
					return
				}
				c.conn = conn
				c.ch = ch
				ch.Qos(count, size, false)
				// re-establish our connections
				for _, ex := range c.Exchanges {
					ex.ch = ch
				}
				for _, queue := range c.Queues {
					queue.ch = ch
				}
				c.mu.Unlock()
			} else {
				c.mu.Unlock()
				break
			}
		}
	}(conn)

	c.client = redis.NewClient(&redis.Options{Addr: c.RedisURL})
	if err := c.client.Ping().Err(); err != nil {
		return fmt.Errorf("Error connecting to %s. %v", c.RedisURL, err)
	}

	// set any channel prefetch configuration
	if c.Channel.PrefetchCount != nil || c.Channel.PrefetchSize != nil {
		if c.Channel.PrefetchSize != nil {
			size = *c.Channel.PrefetchSize
		}
		if c.Channel.PrefetchCount != nil {
			count = *c.Channel.PrefetchCount
		}
		if err := ch.Qos(count, size, false); err != nil {
			return fmt.Errorf("Error creating channel to %s. setting Qos returned an error. %v", c.MQURL, err)
		}
	}

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

	for name, job := range c.Jobs {
		job.name = name
	}

	c.connected = true

	return nil
}

// Close the configuration and release any opened resources
func (c *Config) Close() error {
	c.mu.Lock()
	if c.connected {
		c.connected = false
		if c.ch != nil {
			if err := c.ch.Close(); err != nil {
				exit := true
				if e, ok := err.(*amqp.Error); ok {
					// 504 is when we're already closed
					if e.Code == 504 {
						exit = false
					}
				}
				if exit {
					c.mu.Unlock()
					c.ch = nil
					return err
				}
			}
			c.ch = nil
		}
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				c.mu.Unlock()
				c.conn = nil
				return err
			}
			c.conn = nil
		}
	}
	c.mu.Unlock()
	return nil
}

// NewConfig will load a config from the reader and return it
func NewConfig(r io.Reader) (*Config, error) {
	dec := json.NewDecoder(r)
	config := &Config{}
	if err := dec.Decode(config); err != nil {
		return nil, fmt.Errorf("error parsing configuration. %v", err)
	}
	if config.MQURL == "" {
		config.MQURL = "amqp://guest:guest@localhost:5672/"
	}
	if config.RedisURL == "" {
		config.RedisURL = "localhost:6379"
	}
	config.mu = sync.RWMutex{}
	return config, nil
}
