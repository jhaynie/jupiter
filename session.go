package jupiter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Message is a wrapper around the MQ message
type Message struct {
	*amqp.Delivery
	session *SessionConnection
}

// Ack will acknowledge a Message
func (m *Message) Ack() {
	m.session.Ack(m.DeliveryTag, false)
}

// Reader returns an io.Reader to the message body
func (m *Message) Reader() io.Reader {
	return bytes.NewReader(m.Body)
}

// SessionConnection composes an amqp.Connection with an amqp.Channel
type SessionConnection struct {
	*amqp.Connection
	*amqp.Channel
	consumerTag string
}

// Close tears the connection down, taking the channel with it.
func (s *SessionConnection) Close() error {
	if s.consumerTag != "" {
		s.Cancel(s.consumerTag, false)
		s.consumerTag = ""
	}
	if s.Channel != nil {
		if err := s.Channel.Close(); err != nil {
			// 504 is the code which is channel already closed which we can ignore
			if e, ok := err.(*amqp.Error); !ok || (ok && e.Code != 504) {
				return err
			}
		}
		// wait for the close
		err := <-s.Channel.NotifyClose(make(chan *amqp.Error))
		if err != nil {
			return err
		}
		s.Channel = nil
	}
	if s.Connection != nil {
		if err := s.Connection.Close(); err != nil {
			return err
		}
		s.Connection = nil
	}
	return nil
}

// Setup is called to establish any initialization after connection is established
type Setup func(conn *amqp.Connection, ch *amqp.Channel) error

// Session handles the session details for connection
type Session struct {
	ctx         context.Context
	cancel      context.CancelFunc
	session     chan chan *SessionConnection
	receiving   chan Message
	sending     chan Message
	consumerTag string
}

// Close will close the session and shutdown the connection
func (s Session) Close() {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
}

// Session returns a channel of channels for retrieving a session
func (s Session) Session() <-chan chan *SessionConnection {
	return s.session
}

// ConsumerChannel returns a channel of received messages
func (s Session) ConsumerChannel() <-chan Message {
	return s.receiving
}

// ProducerChannel returns a channel for sending messages
func (s Session) ProducerChannel() chan<- Message {
	return s.sending
}

// Dial will provide a Session which will connect or reconnect the the url
func Dial(c context.Context, url string, setup Setup) *Session {
	ctx, done := context.WithCancel(c)
	sessions := make(chan chan *SessionConnection)
	session := &Session{
		ctx:       ctx,
		cancel:    done,
		session:   sessions,
		receiving: make(chan Message),
		sending:   make(chan Message),
	}

	// create a goroutine that will handle maintaining
	// a valid connection to the server and will automatically
	// reconnect as necessary
	go func() {
		sess := make(chan *SessionConnection)
		var conn *amqp.Connection
		var ch *amqp.Channel
		defer func() {
			if session.consumerTag != "" {
				ch.Cancel(session.consumerTag, false)
				session.consumerTag = ""
			}
			close(sessions)
			close(session.receiving)
			close(session.sending)
			close(sess)
			session.receiving = nil
			session.sending = nil
			done()
		}()
		backoff := Backoff{}

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				return
			}

			var err error

			for {
				conn, err = amqp.Dial(url)
				if err != nil {
					d := backoff.Duration()
					log.Printf("cannot (re)dial: %v: %q, will retry in %v", err, url, d)
					time.Sleep(d)
					continue
				}

				ch, err = conn.Channel()
				if err != nil {
					d := backoff.Duration()
					log.Printf("cannot create channel: %v, will retry in %v", err, d)
					time.Sleep(d)
					continue
				}

				if setup != nil {
					if err = setup(conn, ch); err != nil {
						log.Fatalf("cannot setup channel: %v", err)
					}
				}

				// reset on a successful connection
				backoff.Reset()
				break
			}

			select {
			case sess <- &SessionConnection{conn, ch, ""}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return session
}

// SessionSetup is an optional function that will be called during setup
type SessionSetup func(conn *amqp.Connection, ch *amqp.Channel) error

// NewSession will return a new Session using a specified exchange
func NewSession(ctx context.Context, url, exchange, kind string, durable, autodelete bool, setups ...SessionSetup) *Session {
	setup := func(conn *amqp.Connection, ch *amqp.Channel) error {
		if err := ch.ExchangeDeclare(exchange, kind, durable, autodelete, false, false, nil); err != nil {
			return err
		}
		if setups != nil {
			for _, s := range setups {
				if err := s(conn, ch); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return Dial(ctx, url, setup)
}

// StartPublisher will start the session
func (s Session) StartPublisher(exchange string) chan<- Message {
	messages := s.sending
	acq := make(chan bool)
	go func() {
		for session := range s.Session() {
			var (
				running bool
				reading = messages
				pending = make(chan Message, 1)
				confirm = make(chan amqp.Confirmation, 1)
			)

			// acquire an available session
			pub := <-session

			defer pub.Close()

			// publisher confirms for this channel/connection
			if err := pub.Confirm(false); err != nil {
				log.Println("publisher confirms not supported")
				close(confirm) // confirms not supported, simulate by always nacking
			} else {
				pub.NotifyPublish(confirm)
			}

			if acq != nil {
				acq <- true
				acq = nil
			}

		Publish:
			for {
				var msg Message
				select {
				case confirmed, ok := <-confirm:
					if !ok {
						break Publish
					}
					if !confirmed.Ack {
						log.Printf("nack message %d, body: %q", confirmed.DeliveryTag, string(msg.Body))
					}
					reading = messages

				case msg = <-pending:
					rk := msg.RoutingKey
					msgid := msg.MessageId
					if msgid == "" {
						msgid = randString(64)
					}
					if rk == "" {
						// if there's not routing key, use the message type as the key
						rk = msg.Type
					}
					err := pub.Publish(exchange, rk, false, false, amqp.Publishing{
						Headers:         msg.Headers,
						ContentType:     msg.ContentType,
						ContentEncoding: msg.ContentEncoding,
						DeliveryMode:    msg.DeliveryMode,
						Priority:        msg.Priority,
						CorrelationId:   msg.CorrelationId,
						ReplyTo:         msg.ReplyTo,
						Expiration:      msg.Expiration,
						MessageId:       msgid,
						Timestamp:       msg.Timestamp,
						Type:            msg.Type,
						UserId:          msg.UserId,
						AppId:           msg.AppId,
						Body:            msg.Body,
					})
					// Retry failed, delivery on the next session
					if err != nil {
						pending <- msg
						pub.Close()
						break Publish
					}

				case msg, running = <-reading:
					// all messages consumed
					if !running {
						return
					}
					// work on pending delivery until ack'd
					pending <- msg
					reading = nil
				}
			}
		}
	}()
	<-acq
	return messages
}

// StartConsumer will start consuming messages
func (s Session) StartConsumer(exchange, queue, routingKey string) <-chan Message {
	if s.consumerTag != "" {
		panic("session is already consuming")
	}
	acq := make(chan bool)
	go func() {
		for session := range s.Session() {
			// retrieve a new session which we will hold until
			// the connection is no longer available
			var sub *SessionConnection

			select {
			case sub = <-session:
			case <-s.ctx.Done():
				sub.Close()
				return
			}

			defer func() {
				if s.consumerTag != "" {
					sub.Cancel(s.consumerTag, false)
					s.consumerTag = ""
				}
				sub.Close()
			}()

			if _, err := sub.QueueDeclare(queue, false, true, true, false, nil); err != nil {
				log.Fatalf("cannot consume from exclusive queue: %q, %v", queue, err)
				return
			}

			if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
				log.Fatalf("cannot consume without a binding to exchange: %q, %v", exchange, err)
				return
			}

			s.consumerTag = randString(32)
			sub.consumerTag = s.consumerTag

			deliveries, err := sub.Consume(queue, s.consumerTag, false, true, false, false, nil)
			if err != nil {
				log.Printf("cannot consume from: %q, %v", queue, err)
				return
			}

			if acq != nil {
				acq <- true
				acq = nil
			}

			// listen for each message on the channel and then send
			// to the receiving channel
			for {
				select {
				case <-s.ctx.Done():
					{
						return
					}
				case msg := <-deliveries:
					{
						s.receiving <- Message{&msg, sub}
					}
				}
			}
		}
	}()
	<-acq
	return s.ConsumerChannel()
}

// WithPublishOption can be called to set extra options
type WithPublishOption func(delivery *amqp.Delivery)

// WithType will set the message type
func WithType(t string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.Type = t
	}
}

// WithCorrelation will set the correlation id
func WithCorrelation(id string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.CorrelationId = id
	}
}

// WithReply will set the reply to
func WithReply(r string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.ReplyTo = r
	}
}

// WithContentType will set the content type
func WithContentType(ct string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.ContentType = ct
	}
}

// WithEncoding will set the content encoding
func WithEncoding(ce string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.ContentEncoding = ce
	}
}

// WithMessageID will set the message id
func WithMessageID(id string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.MessageId = id
	}
}

// WithAppID will set the app id
func WithAppID(id string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.AppId = id
	}
}

// WithUserID will set the user id
func WithUserID(id string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.UserId = id
	}
}

// WithDeliveryMode will set the delivery mode
func WithDeliveryMode(mode uint8) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.DeliveryMode = mode
	}
}

// WithExpiration will set the expiration
func WithExpiration(exp time.Duration) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.Expiration = fmt.Sprintf("%d", int64(time.Until(time.Now().Add(exp))/time.Millisecond))
	}
}

// WithHeader will set a message header
func WithHeader(key, value string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		if delivery.Headers == nil {
			delivery.Headers = amqp.Table{}
		}
		delivery.Headers[key] = value
	}
}

// WithRouting with set the message routing key
func WithRouting(key string) WithPublishOption {
	return func(delivery *amqp.Delivery) {
		delivery.RoutingKey = key
	}
}

// Publish will publish a message on the channel
func (s Session) Publish(body []byte, opts ...WithPublishOption) error {
	msg := &amqp.Delivery{Body: body}
	for _, o := range opts {
		o(msg)
	}
	s.ProducerChannel() <- Message{msg, nil}
	return nil
}
