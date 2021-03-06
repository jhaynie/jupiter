package jupiter

import (
	"encoding/json"
	"fmt"
)

var (
	registry = make(map[string]Worker)
)

type echoWorker struct {
}

// Work will simply echo back out the json it receives
func (w *echoWorker) Work(message WorkMessage, done Done) error {
	defer done(nil)
	dec := json.NewDecoder(message.Reader())
	var buf json.RawMessage
	if err := dec.Decode(&buf); err != nil {
		return err
	}
	_, err := message.Writer().Write(buf)
	return err
}

func init() {
	Register("echo", &echoWorker{})
}

// Register is called to register a Worker by name. This is not thread safe and generally
// should be called from the init function on startup. Only one worker can be registered
// for a given name
func Register(name string, worker Worker) error {
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
