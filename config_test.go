package jupiter

import (
	"strings"
	"sync"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestBasicConfig(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"pinpt.exchange.test.github": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "github.#",
					"exchange": "pinpt.exchange.test.main"
				}
			]
		}
	},
	"queues": {
		"pinpt.github.test.commit": {
			"autodelete": true,
			"exchange": "pinpt.exchange.test.github",
			"routing": "github.commit",
			"durable": false
		},
		"myqueue": {
			"autodelete": true,
			"private": true,
			"exchange": "pinpt.exchange.test.github",
			"routing": "#",
			"durable": false
		}
	}
}`)
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	name, ch, err := config.Queues["myqueue"].Consume(true, true, false)
	assert.Nil(err)
	assert.NotEmpty(name)
	assert.NotNil(ch)
	assert.Nil(config.Publish("github.commit", amqp.Publishing{
		Body: []byte("hello"),
	}))
	msg := <-ch
	assert.NotNil(msg)
	assert.Equal("hello", string(msg.Body))
	assert.Nil(config.Queues["myqueue"].Cancel(name))
	name, ch, err = config.Queues["pinpt.github.test.commit"].Consume(true, true, false)
	assert.Nil(err)
	assert.NotEmpty(name)
	assert.NotNil(ch)
	msg = <-ch
	assert.NotNil(msg)
	assert.Equal("hello", string(msg.Body))
	assert.Nil(config.Queues["pinpt.github.test.commit"].Cancel(name))
	assert.Nil(config.Close())
}

func TestMultipleWorkerConfig(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"pinpt.exchange.test.github": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "github.#",
					"exchange": "pinpt.exchange.test.main"
				}
			]
		}
	},
	"queues": {
		"pinpt.github.test.commit": {
			"autodelete": true,
			"exchange": "pinpt.exchange.test.github",
			"routing": "github.#",
			"durable": false
		}
	}
}`)
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	name1, ch1, err := config.Queues["pinpt.github.test.commit"].Consume(true, false, false)
	assert.Nil(err)
	assert.NotEmpty(name1)
	assert.NotNil(ch1)
	name2, ch2, err := config.Queues["pinpt.github.test.commit"].Consume(true, false, false)
	assert.Nil(err)
	assert.NotEmpty(name2)
	assert.NotNil(ch2)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for msg := range ch1 {
			assert.Equal("hello", string(msg.Body))
			wg.Done()
		}
	}()
	go func() {
		for msg := range ch2 {
			assert.Equal("hello", string(msg.Body))
			wg.Done()
		}
	}()
	assert.Nil(config.Publish("github.commit", amqp.Publishing{
		Body: []byte("hello"),
	}))
	wg.Wait()
	assert.Nil(config.Queues["pinpt.github.test.commit"].Cancel(name1))
	assert.Nil(config.Queues["pinpt.github.test.commit"].Cancel(name2))
	assert.Nil(config.Close())
}

func TestJobWorkerConfig(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main"
				}
			]
		},
		"echoresult": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
			"autodelete": true,
			"exchange": "echoresult",
			"routing": "#",
			"durable": false
		}
	},
	"jobs": {
		"echo": {
			"worker": "echo",
			"queue": "echo",
			"publish": "echo.result"
		},
		"echoresult": {
			"worker": "echo",
			"queue": "echoresult"
		}
	}
}`)
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	echoName, echoCh, err := config.Queues["echo"].Consume(true, false, false)
	assert.Nil(err)
	assert.NotEmpty(echoName)
	assert.NotNil(echoCh)
	assert.NotNil(config.Jobs["echo"])
	assert.NotNil(config.Jobs["echoresult"])
	assert.Equal("echo.result", config.Jobs["echo"].Publish)
	assert.Equal("", config.Jobs["echoresult"].Publish)
	assert.Equal("echo", config.Jobs["echo"].Queue)
	assert.Equal("echoresult", config.Jobs["echoresult"].Queue)
	assert.Equal("echo", config.Jobs["echo"].Worker)
	assert.Equal("echo", config.Jobs["echoresult"].Worker)
	assert.Nil(config.Queues["echo"].Cancel(echoName))
	assert.Nil(config.Close())
}

func TestConfigWithQoS(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"channel": {
		"prefetch_count": 1
	},
	"exchanges": {
		"pinpt.exchange.test.main": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"pinpt.exchange.test.github": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "github.#",
					"exchange": "pinpt.exchange.test.main"
				}
			]
		}
	},
	"queues": {
		"pinpt.github.test.commit": {
			"autodelete": true,
			"exchange": "pinpt.exchange.test.github",
			"routing": "github.#",
			"durable": false
		},
		"myqueue": {
			"autodelete": true,
			"private": true,
			"exchange": "pinpt.exchange.test.github",
			"routing": "#",
			"durable": false
		}
	}
}`)
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	name, ch, err := config.Queues["myqueue"].Consume(true, true, false)
	assert.Nil(err)
	assert.NotEmpty(name)
	assert.NotNil(ch)
	assert.NotNil(config.Channel.PrefetchCount)
	assert.Nil(config.Channel.PrefetchSize)
	assert.Equal(1, *config.Channel.PrefetchCount)
	assert.Nil(config.Publish("github.commit", amqp.Publishing{
		Body: []byte("hello"),
	}))
	msg := <-ch
	assert.NotNil(msg)
	assert.Equal("hello", string(msg.Body))
	assert.Nil(config.Queues["myqueue"].Cancel(name))
	name, ch, err = config.Queues["pinpt.github.test.commit"].Consume(true, true, false)
	assert.Nil(err)
	assert.NotEmpty(name)
	assert.NotNil(ch)
	msg = <-ch
	assert.NotNil(msg)
	assert.Equal("hello", string(msg.Body))
	assert.Nil(config.Queues["pinpt.github.test.commit"].Cancel(name))
	assert.Nil(config.Close())
}
