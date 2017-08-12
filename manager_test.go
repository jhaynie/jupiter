package jupiter

import (
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type echoBack struct {
	buf []byte
	wg  sync.WaitGroup
}

func (e *echoBack) Work(msg WorkMessage, done Done) error {
	defer done(nil)
	buf, err := ioutil.ReadAll(msg.Reader())
	e.buf = buf
	e.wg.Done()
	return err
}

type echoBackAsync struct {
	buf []byte
	wg  sync.WaitGroup
}

func (e *echoBackAsync) Work(msg WorkMessage, done Done) error {
	go func() {
		time.Sleep(time.Second)
		buf, err := ioutil.ReadAll(msg.Reader())
		e.buf = buf
		done(err)
		e.wg.Done()
	}()
	return nil
}

type echoBackAsyncError struct {
	buf []byte
	wg  sync.WaitGroup
}

func (e *echoBackAsyncError) Work(msg WorkMessage, done Done) error {
	go func() {
		time.Sleep(time.Second)
		buf, err := ioutil.ReadAll(msg.Reader())
		e.buf = buf
		// should be an error
		msg.Config.Publish("", amqp.Publishing{UserId: "123", DeliveryMode: 10})
		done(err)
		e.wg.Done()
	}()
	return nil
}

type echoBackRedis struct {
	result string
	wg     sync.WaitGroup
}

func (e *echoBackRedis) Work(msg WorkMessage, done Done) error {
	defer done(nil)
	result, err := msg.Config.RedisClient().Get(msg.AppID).Result()
	e.result = result
	e.wg.Done()
	return err
}

func TestJobWorker(t *testing.T) {
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
			"worker": "echoresult",
			"queue": "echoresult"
		}
	}
}`)
	echo := &echoBack{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", amqp.Publishing{
		Body: []byte(`{"hi":"heya"}`),
	})
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}

func TestAsyncJobWorker(t *testing.T) {
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
			"worker": "echoresult",
			"queue": "echoresult"
		}
	}
}`)
	echo := &echoBackAsync{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", amqp.Publishing{
		Body: []byte(`{"hi":"heya"}`),
	})
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}

func TestJobResultRedis(t *testing.T) {
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
			"publish": "echo.result",
			"destination": "both"
		},
		"echoresult": {
			"worker": "echoresult",
			"queue": "echoresult"
		}
	}
}`)
	echo := &echoBackRedis{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", amqp.Publishing{
		Body: []byte(`{"hi":"heya"}`),
	})
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, echo.result)
	mgr.Close()
	assert.Nil(config.Close())
}

func TestAutoConnect(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{}`)
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.False(config.IsConnected())
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	assert.True(config.IsConnected())
	mgr.Close()
	assert.False(config.IsConnected())
}

func TestAutoReconnect(t *testing.T) {
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
			"worker": "echoresult",
			"queue": "echoresult"
		}
	}
}`)
	echo := &echoBackAsyncError{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	assert.Nil(config.Connect())
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", amqp.Publishing{
		Body: []byte(`{"hi":"heya"}`),
	})
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}
