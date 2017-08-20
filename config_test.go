package jupiter

import (
	"strings"
	"testing"

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
	assert.Nil(config.Close())
}
