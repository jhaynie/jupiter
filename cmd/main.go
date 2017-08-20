package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jhaynie/jupiter"
	"github.com/streadway/amqp"
)

var url = flag.String("url", "amqp:///", "AMQP url for both the publisher and subscriber")

// exchange binds the publishers to the subscribers
const exchange = "pubsub"

// identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.
func identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

func main() {
	flag.Parse()

	ctx, done := context.WithCancel(context.Background())

	// setup is called to establish connection to the exchange
	setup := func(conn *amqp.Connection, ch *amqp.Channel) error {
		if err := ch.ExchangeDeclare(exchange, "topic", false, true, false, false, nil); err != nil {
			return err
		}
		return nil
	}

	defer done()

	// create a session for publishing data from stdin
	session1 := jupiter.Dial(ctx, *url, setup)
	go func() {
		scan := bufio.NewReader(os.Stdin)
		session1.StartPublisher(exchange)
		for {
			line, err := scan.ReadString('\n')
			if err == io.EOF {
				break
			}
			session1.Publish([]byte(strings.TrimSpace(line)), jupiter.WithType("foo.bar"))
		}
	}()

	// create a session for subscribing and printing to stdout
	session2 := jupiter.Dial(ctx, *url, setup)
	go func() {
		queue := identity()
		for msg := range session2.StartConsumer(exchange, queue, "#") {
			fmt.Println(">>", string(msg.Body))
		}
	}()

	fmt.Println("type a message and it should publish and then subscribe and print back out ...")

	<-ctx.Done()
}
