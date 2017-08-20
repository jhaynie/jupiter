package jupiter

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"

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
	buf   []byte
	wg    sync.WaitGroup
	delay bool
}

func (e *echoBackAsync) Work(msg WorkMessage, done Done) error {
	go func() {
		if e.delay {
			time.Sleep(time.Second)
		}
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
		msg.Config.Publish("abc", []byte(""), WithAppID("123"), WithDeliveryMode(10))
		done(err)
		e.wg.Done()
	}()
	return nil
}

type echoBackAsyncError2 struct {
	wg sync.WaitGroup
}

func (e *echoBackAsyncError2) Work(msg WorkMessage, done Done) error {
	go func() {
		done(fmt.Errorf("error"))
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
		"pinpt.exchange.test.main1": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo1": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main1"
				}
			]
		},
		"echoresult1": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main1"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo1",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
			"autodelete": true,
			"exchange": "echoresult1",
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
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	assert.Nil(config.PublishJSON("echo", map[string]string{"hi": "heya"}))
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}

func TestAsyncJobWorker(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main2": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo2": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main2"
				}
			]
		},
		"echoresult2": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main2"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo2",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
 			"autodelete": true,
			"exchange": "echoresult2",
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
	echo := &echoBackAsync{wg: sync.WaitGroup{}, delay: true}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", []byte(`{"hi":"heya"}`))
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}

func TestJobResultRedis(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main3": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo3": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main3"
				}
			]
		},
		"echoresult3": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main3"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo3",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
			"autodelete": true,
			"exchange": "echoresult3",
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
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", []byte(`{"hi":"heya"}`))
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, echo.result)
	mgr.Close()
	assert.Nil(config.Close())
}

func TestAutoReconnect(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main4": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo4": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main4"
				}
			]
		},
		"echoresult4": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main4"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo4",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
 			"autodelete": true,
			"exchange": "echoresult4",
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
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	config.Publish("echo", []byte(`{"hi":"heya"}`))
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}

func TestAsyncJobWorkerMulti(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main5": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo5": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main5"
				}
			]
		},
		"echoresult5": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main5"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo5",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
 			"autodelete": true,
			"exchange": "echoresult5",
			"routing": "#",
			"durable": false
		}
	},
	"jobs": {
		"echo": {
			"worker": "echo",
			"queue": "echo",
			"publish": "echo.result",
			"concurrency": 100
		},
		"echoresult": {
			"worker": "echoresult",
			"queue": "echoresult",
			"concurrency": 100
		}
	},
	"channel": {
		"prefetch_count": 100
	}
}`)
	total := 1000
	echo := &echoBackAsync{wg: sync.WaitGroup{}, delay: false}
	echo.wg.Add(total)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	for i := 0; i < total; i++ {
		go config.PublishJSONString("echo", `{"hi":"heya"}`)
	}
	echo.wg.Wait()
	assert.Equal(`{"hi":"heya"}`, string(echo.buf))
	mgr.Close()
	assert.Nil(config.Close())
}

func TestErrorChannel(t *testing.T) {
	assert := assert.New(t)
	r := strings.NewReader(`{
	"exchanges": {
		"pinpt.exchange.test.main6": {
			"type": "topic",
			"autodelete": true,
			"default": true
		},
		"echo6": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo",
					"exchange": "pinpt.exchange.test.main6"
				}
			]
		},
		"echoresult6": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.test.main6"
				}
			]
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "echo6",
			"routing": "#",
			"durable": false
		},
		"echoresult": {
 			"autodelete": true,
			"exchange": "echoresult6",
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
	echo := &echoBackAsyncError2{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := NewConfig(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.MQURL)
	assert.Equal("localhost:6379", config.RedisURL)
	defer config.Close()
	mgr, err := NewManager(config)
	assert.Nil(err)
	assert.NotNil(mgr)
	defer mgr.Close()
	assert.Nil(config.PublishJSONString("echo", `{"hi":"heya"}`))
	echo.wg.Wait()
	err = <-mgr.ErrorChannel()
	assert.Error(err, "should have been an error")
	assert.Equal("error", err.Error())
	mgr.Close()
	assert.Nil(config.Close())
}
