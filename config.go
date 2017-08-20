package jupiter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	name    string
	session *Session
}

// Publish will publish to the exchange
func (e *Exchange) Publish(body []byte, opts ...WithPublishOption) error {
	return e.session.Publish(body, opts...)
}

// Name returns the name of the exchange
func (e *Exchange) Name() string {
	return e.name
}

// Close the related resources
func (e *Exchange) Close() {
	if e.session != nil {
		e.session.Close()
		e.session = nil
	}
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

	name         string
	exchangeType string
	config       *Config
	session      *Session
}

// Name returns the queue name
func (q *Queue) Name() string {
	return q.name
}

// ExchangeType return the type of the exchange this queue belongs to
func (q *Queue) ExchangeType() string {
	return q.exchangeType
}

// Close the queue's resources
func (q *Queue) Close() {
	if q.session != nil {
		q.session.Close()
		q.session = nil
	}
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

	exchange *Exchange
	client   *redis.Client
}

// RedisClient will return the redis client instance
func (c *Config) RedisClient() *redis.Client {
	return c.client
}

// Publish will publish a message to the default queue
func (c *Config) Publish(name string, body []byte, opts ...WithPublishOption) error {
	if c.exchange == nil {
		return fmt.Errorf("no default exchange found")
	}
	if opts == nil {
		opts = []WithPublishOption{}
	}
	opts = append(opts, WithType(name))
	return c.exchange.Publish(body, opts...)
}

// PublishJSONString will publish a JSON string to the default queue
func (c *Config) PublishJSONString(name string, body string, opts ...WithPublishOption) error {
	if c.exchange == nil {
		return fmt.Errorf("no default exchange found")
	}
	if opts == nil {
		opts = []WithPublishOption{}
	}
	opts = append(opts, WithContentType("application/json"), WithType(name))
	return c.exchange.Publish([]byte(body), opts...)
}

// PublishJSON will publish an object as an encoded JSON string to the default queue
func (c *Config) PublishJSON(name string, body interface{}, opts ...WithPublishOption) error {
	if c.exchange == nil {
		return fmt.Errorf("no default exchange found")
	}
	return c.PublishJSONString(name, stringify(body), opts...)
}

// Close the connections
func (c *Config) Close() error {
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			return err
		}
		c.client = nil
	}
	for _, q := range c.Queues {
		q.Close()
	}
	for _, ex := range c.Exchanges {
		ex.Close()
	}
	return nil
}

// Setup will use the channel to setup the Exchanges and Queues
func (c *Config) Setup(ctx context.Context) error {
	// create all our exchanges
	exchanges := make(map[string]bool)
	for name, ex := range c.Exchanges {
		ex.name = name
		exchanges[name] = true
		setup := func(conn *amqp.Connection, ch *amqp.Channel) error {
			count := 1
			size := 0
			// set any channel prefetch configuration
			if c.Channel.PrefetchCount != nil || c.Channel.PrefetchSize != nil {
				if c.Channel.PrefetchSize != nil {
					size = *c.Channel.PrefetchSize
				}
				if c.Channel.PrefetchCount != nil {
					count = *c.Channel.PrefetchCount
				}
			}
			if err := ch.Qos(count, size, false); err != nil {
				return fmt.Errorf("Error creating channel to %s. setting Qos returned an error. %v", c.MQURL, err)
			}
			// now bind the exchanges
			if len(ex.Bindings) > 0 {
				for _, binding := range ex.Bindings {
					if !exchanges[binding.Exchange] {
						// we need to make sure that the exchange is declared if not already declared
						other := c.Exchanges[binding.Exchange]
						// fmt.Printf("declaring %s for %s\n", binding.Exchange, name)
						if err := ch.ExchangeDeclare(binding.Exchange, other.Type, other.Durable, other.Autodelete, false, false, nil); err != nil {
							return fmt.Errorf("error declaring exchange named `%s`. %v", binding.Exchange, err)
						}
						exchanges[binding.Exchange] = true
					}
					// fmt.Printf("binding %s -> %s via %s\n", binding.Exchange, name, binding.Routing)
					if err := ch.ExchangeBind(name, binding.Routing, binding.Exchange, false, nil); err != nil {
						return fmt.Errorf("error binding exchange named `%s` to `%s`. %v", name, binding.Exchange, err)
					}
				}
			}
			return nil
		}
		if c.exchange == nil && ex.Default {
			c.exchange = ex
		}
		ex.session = NewSession(ctx, c.MQURL, name, ex.Type, ex.Durable, ex.Autodelete, setup)
		ex.session.StartPublisher(name)
	}

	// now create queues
	for name, queue := range c.Queues {
		ex := c.Exchanges[queue.Exchange]
		if ex == nil {
			return fmt.Errorf("error creating queue named `%s`. Invalid exchange named `%s`", name, queue.Exchange)
		}
		queue.config = c
		queue.exchangeType = ex.Type
	}

	for name, job := range c.Jobs {
		job.name = name
	}

	c.client = redis.NewClient(&redis.Options{Addr: c.RedisURL})
	if err := c.client.Ping().Err(); err != nil {
		return fmt.Errorf("Error connecting to %s. %v", c.RedisURL, err)
	}

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
	return config, nil
}
