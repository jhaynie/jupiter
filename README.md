# Jupiter [![CircleCI](https://circleci.com/gh/jhaynie/jupiter/tree/master.svg?style=svg)](https://circleci.com/gh/jhaynie/jupiter/tree/master)

Jupiter is a very simplistic, queue-based job worker framework.

It uses RabbitMQ to coordinate jobs across N workers.

Workers register to handle jobs by name and have a simplistic in/out type interface.

Jupiter preferes declarative configuration over imperative code to configure the orchestration of work.

![jupiter](_images/jupiter.jpg)

## Install

```shell
go get -u github.com/jhaynie/jupiter
```

## Requirements

You'll need access to the following for Jupiter to function:

- RabbitMQ
- Redis

The easiest way for testing is to run both in a docker container such as:

```shell
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:management
docker run -d -p 6379:6379 redis
```

By default, the configuration will talk with both RabbitMQ and Redis on localhost.

To change the location of the servers, you can set the following in the root of your configuration:

- `mqurl`: defaults to `amqp://guest:guest@localhost:5672/`
- `redisurl`: defaults to `localhost:6379`

## Use

First, you need to register one or more job workers.  Workers are responsible for processing incoming data (opaque to Jupiter) and optionally, returning results.

Let's create a job that will simply echo out the incoming message body:

```go
import (
	"fmt"
	"io"
	"ioutil"

	"github.com/jhaynie/jupiter"
)

type myJob struct {
}

func (j *myJob) Work(msg jupiter.WorkMessage, done jupiter.Done) error {
	defer done(nil)
	buf, err := ioutil.ReadAll(msg.Reader())
	if err != nil {
		return err
	}
	fmt.Println(string(buf))
	return nil
}

func init() {
	jupiter.Register("myjob", &myJob{})
}
```

Make sure you load this package in your program to ensure that the job is registered.

Now we need to create a simple configuration:

```json
{
	"exchanges": {
		"main": {
			"type": "topic",
			"autodelete": true,
			"default": true
		}
	},
	"queues": {
		"echo": {
			"autodelete": true,
			"exchange": "main",
			"routing": "#",
			"durable": false
		}
	},
	"jobs": {
		"echo": {
			"worker": "myjob",
			"queue": "echo"
		}
	}
}
```

This configuration will setup 1 exchange named `main` and one queue bound to the exchange named `echo`.  We're going create a job named `echo` using the worker `myjob` which we created by listening to queue `echo` for job requests.

Now we need to create a Work Manager to manager the work.  First we need to create our configuration using our preferred `io.Reader` interface to pass in the configuration.  Then, we going to pass the configuration into `work.New` to create a new Work Manager.  For testing, we are going to just publish a message using the key `echo`. However, note that our configuration above used `#` which is a wildcard topic and will match any message name.

```golang
config, err := jupiter.NewConfig(r)
if err != nil {
	return err
}
defer config.Close()
mgr, err := jupiter.NewManager(config)
if err != nil {
	return err
}
defer mgr.Close()
config.PublishJSONString("echo", `{"hi":"heya"}`)
```

If it works, you should see `{"hi":"heya"}` in the console (based on our `fmt.Println` in our job code above).

To publish a response, we would add a `publish` key in our job config and we would write the response data to our `io.Writer` (2nd paramter).

For example, to publish the response message `myresult`, you would change to:

```json
"jobs": {
	"echo": {
		"worker": "myjob",
		"queue": "echo",
		"publish": "myresult"
	}
}
```

And then in our worker body we might:

```golang
func (j *myJob) Work(msg jupiter.WorkMessage, done jupiter.Done) error {
	defer done(nil)
	_, err := ioutil.ReadAll(msg.Reader())
	if err != nil {
		return err
	}
	_, err = msg.Writer().Write([]byte("{success:true}"))
	return err
}
```

## Async Jobs

To create an asynchronous job, you can use the `done` argument to signal when you're completed.  For example, this job will wait 1 second and then respond:

```golang
func (j *myJob) Work(msg jupiter.WorkMessage, done jupiter.Done) error {
	go func() {
		time.Sleep(time.Second)
		_, err := msg.Writer().Write([]byte("{success:true}"))
		done(err)
	}()
	return nil
}
```

> NOTE: you must invoke done when you are complete in both the synchronous and asynchronous cases.

## Storing Results in different locations

By default, if you have a `publish` property in your `job`, it will publish any data written to `io.Writer` as a RabbitMQ message.

You can change the destination to publish results to `redis` by changing the `destination` property of your `job` to `redis`.  Also, if you the value to `both` it will publish to both RabbitMQ and Redis.

The key will be in the format `jupiter.<MSGID>.<SHA256 of Job Name>.result`.

If you would like to write different messages to RabbitMQ and Redis, you can use separate io.Writer for each:

```golang
func (j *myJob) Work(msg jupiter.WorkMessage, done jupiter.Done) error {
	// write to redis
	_, err := msg.RedisWriter().Write([]byte("{success:true}"))
	if err != nil {
		return err
	}
	// write to rabbit
	_, err = msg.MQWriter().Write([]byte("{id:123}"))
	if err != nil {
		return err
	}
	return nil
}
```

> NOTE: using `msg.Writer()` writes to both destinations automatically using a `io.MultiWriter`

As a convenience, the outgoing published to RabbitMQ will have the `AppId` property set to the key used to set the result in Redis.

## Message Expiration

By default, the result will expiry in one day.  This value will be the Message TTL for the RabbitMQ message and the value will be used as the expiration if the result is stored in Redis.

To change the expiration, set the `expires` property in your `job` to a value such as `3d` or `2m` or `10s`.

