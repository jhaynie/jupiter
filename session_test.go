package jupiter

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSessionConnect(t *testing.T) {
	session := NewSession(context.Background(), "amqp:///", "pubsub", "direct", false, true)
	session.Close()
}

func TestSessionPubSub(t *testing.T) {
	assert := assert.New(t)
	ctx, done := context.WithCancel(context.Background())
	defer done()
	session1 := NewSession(ctx, "amqp:///", "pubsub", "topic", false, true)
	defer session1.Close()
	msg := session1.StartConsumer("pubsub", "", "#")
	session2 := NewSession(ctx, "amqp:///", "pubsub", "topic", false, true)
	defer session2.Close()
	session2.StartPublisher("pubsub")
	session2.Publish([]byte("hello"), WithType("foo.bar"))
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for m := range msg {
			assert.Equal("hello", string(m.Body))
			wg.Done()
			break
		}
	}()
	wg.Wait()
}

func TestSessionPubSubMulti(t *testing.T) {
	assert := assert.New(t)
	ctx, done := context.WithCancel(context.Background())
	defer done()
	session1 := NewSession(ctx, "amqp:///", "pubsub", "topic", false, true)
	defer session1.Close()
	msg := session1.StartConsumer("pubsub", "", "#")
	session2 := NewSession(ctx, "amqp:///", "pubsub", "topic", false, true)
	defer session2.Close()
	session2.StartPublisher("pubsub")
	x := 5000
	wg := sync.WaitGroup{}
	wg.Add(x)
	for i := 0; i < x; i++ {
		session2.Publish([]byte("hello"), WithType("foo.bar"))
		go func() {
			for m := range msg {
				assert.Equal("hello", string(m.Body))
				wg.Done()
				break
			}
		}()
	}
	wg.Wait()
}

func TestSessionPubSubSame(t *testing.T) {
	// use the same session for pub sub
	assert := assert.New(t)
	ctx, done := context.WithCancel(context.Background())
	defer done()
	session := NewSession(ctx, "amqp:///", "pubsub", "topic", false, true)
	defer session.Close()
	msg := session.StartConsumer("pubsub", "", "#")
	session.StartPublisher("pubsub")
	x := 5000
	wg := sync.WaitGroup{}
	wg.Add(x)
	for i := 0; i < x; i++ {
		session.Publish([]byte("hello"), WithType("foo.bar"))
		go func() {
			for m := range msg {
				assert.Equal("hello", string(m.Body))
				wg.Done()
				break
			}
		}()
	}
	wg.Wait()
}
