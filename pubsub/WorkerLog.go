package pubsub

import (
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub/v2"
)

var (
	workerLog     map[string]*pubsub.Message = make(map[string]*pubsub.Message)
	workerLogLock sync.Mutex
)

func QueueLength() int {
	workerLogLock.Lock()
	defer workerLogLock.Unlock()
	return len(workerLog)
}

func deleteWorkerLog(id string) {
	workerLogLock.Lock()
	defer workerLogLock.Unlock()
	delete(workerLog, id)
}

func writeWorkerLog(message *pubsub.Message) error {
	workerLogLock.Lock()
	defer workerLogLock.Unlock()
	if _, has := workerLog[message.ID]; has {
		return fmt.Errorf("WorkerLog entry exists for %s", message.ID)
	}

	workerLog[message.ID] = message
	return nil
}
