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

func (j *myJob) Work(msg jupiter.WorkMessage, in io.Reader, out io.Writer, done jupiter.Done) error {
	defer done(nil)
	buf, err := ioutil.ReadAll(in)
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
config.Publish("echo", amqp.Publishing{
	Body: []byte(`{"hi":"heya"}`),
})
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
func (j *myJob) Work(msg jupiter.WorkMessage, in io.Reader, out io.Writer, done jupiter.Done) error {
	defer done(nil)
	_, err := ioutil.ReadAll(in)
	if err != nil {
		return err
	}
	_, err = out.Write([]byte("{success:true}"))
	return err
}
```

To create an asynchronous job, you can use the `done` argument to signal when you're completed.  For example, this job will wait 1 second and then respond:

```golang
func (j *myJob) Work(msg jupiter.WorkMessage, in io.Reader, out io.Writer, done jupiter.Done) error {
	go func() {
		time.Sleep(time.Second)
		_, err := out.Write([]byte("{success:true}"))
		done(err)
	}()
	return nil
}
```

> NOTE: you must invoke done when you are complete in both the synchronous and asynchronous cases.