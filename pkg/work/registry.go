package work

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/jhaynie/jupiter/pkg/types"
)

var (
	registry = make(map[string]types.Worker)
)

type echoWorker struct {
}

// Work will simply echo back out the json it receives
func (w *echoWorker) Work(in io.Reader, out io.Writer) error {
	dec := json.NewDecoder(in)
	var buf json.RawMessage
	if err := dec.Decode(&buf); err != nil {
		return err
	}
	_, err := out.Write(buf)
	return err
}

func init() {
	Register("echo", &echoWorker{})
}

// Register is called to register a Worker by name. This is not thread safe and generally
// should be called from the init function on startup. Only one worker can be registered
// for a given name
func Register(name string, worker types.Worker) error {
	if registry[name] != nil {
		return fmt.Errorf("worker named `%s` already registered", name)
	}
	registry[name] = worker
	return nil
}

// Unregister will remove a registered worker by name. This is not thread safe
func Unregister(name string) error {
	delete(registry, name)
	return nil
}

// autoregister is called when a new worker manager is created to automatically register
// all jobs in the registry
func autoregister(manager *WorkerManager) error {
	for name, worker := range registry {
		if err := manager.Register(name, worker); err != nil {
			return err
		}
	}
	return nil
}
