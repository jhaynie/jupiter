package jupiter

import (
	"io"
)

// Done is called for async workers
type Done func(err error)

// WorkMessage is details about a message
type WorkMessage struct {
	Config          *Config
	MessageType     string
	AppID           string
	MessageID       string
	CorrelationID   string
	ContentType     string
	ContentEncoding string

	reader      io.Reader
	redisWriter io.Writer
	mqWriter    io.Writer
	job         *Job
}

// Reader returns an io.Reader for reading a work message body
func (m *WorkMessage) Reader() io.Reader {
	return m.reader
}

// RedisWriter will return a writer for writing data to redis
func (m *WorkMessage) RedisWriter() io.Writer {
	return m.redisWriter
}

// MQWriter will return a writer for writing data to RabbitMQ
func (m *WorkMessage) MQWriter() io.Writer {
	return m.mqWriter
}

// Writer will return a combined writer that will write the same message to both RabbitMQ and Redis writers
func (m *WorkMessage) Writer() io.Writer {
	return io.MultiWriter(m.redisWriter, m.mqWriter)
}

// ResultKey will return the unique result key which will be stored in the `AppId` of the MQ or be used as
// the key when storing the result in Redis
func (m *WorkMessage) ResultKey() string {
	return "jupiter." + m.MessageID + "." + hashStrings(m.job.Name()) + ".result"
}

// Worker is an interface that workers implement to handle work
type Worker interface {
	Work(msg WorkMessage, done Done) error
}

// Manager is responsible for handling assigning jobs to Worker
type Manager interface {
	Register(name string, worker Worker) error
}
