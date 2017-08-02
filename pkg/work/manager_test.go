package work

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/jhaynie/jupiter/pkg/config"
	"github.com/jhaynie/jupiter/pkg/types"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type echoBack struct {
	buf []byte
	wg  sync.WaitGroup
}

func (e *echoBack) Work(in io.Reader, out io.Writer, done types.Done) error {
	defer done(nil)
	buf, err := ioutil.ReadAll(in)
	e.buf = buf
	e.wg.Done()
	return err
}

type echoBackAsync struct {
	buf []byte
	wg  sync.WaitGroup
}

func (e *echoBackAsync) Work(in io.Reader, out io.Writer, done types.Done) error {
	go func() {
		time.Sleep(time.Second)
		buf, err := ioutil.ReadAll(in)
		e.buf = buf
		done(err)
		e.wg.Done()
	}()
	return nil
}

func TestJobWorker(t *testing.T) {
	assert := assert.New(t)
	r := bytes.NewBuffer([]byte(`{
	"exchanges": {
		"pinpt.exchange.main": {
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
					"exchange": "pinpt.exchange.main"
				}
			]
		},
		"echoresult": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.main"
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
}`))
	echo := &echoBack{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := config.New(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.URL)
	assert.Nil(config.Connect())
	defer config.Close()
	mgr, err := New(config)
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
	r := bytes.NewBuffer([]byte(`{
	"exchanges": {
		"pinpt.exchange.main": {
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
					"exchange": "pinpt.exchange.main"
				}
			]
		},
		"echoresult": {
			"type": "topic",
			"autodelete": true,
			"bind": [
				{
					"routing": "echo.result",
					"exchange": "pinpt.exchange.main"
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
}`))
	echo := &echoBackAsync{wg: sync.WaitGroup{}}
	echo.wg.Add(1)
	Register("echoresult", echo)
	defer Unregister("echoresult")
	config, err := config.New(r)
	assert.Nil(err)
	assert.NotNil(config)
	assert.Equal("amqp://guest:guest@localhost:5672/", config.URL)
	assert.Nil(config.Connect())
	defer config.Close()
	mgr, err := New(config)
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
