package jupiter

import (
	"io"
)

// Done is called for async workers
type Done func(err error)

// Worker is an interface that workers implement to handle work
type Worker interface {
	Work(in io.Reader, out io.Writer, done Done) error
}

// Manager is responsible for handling assigning jobs to Worker
type Manager interface {
	Register(name string, worker Worker) error
}
