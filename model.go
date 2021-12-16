package distr_ep

import "fmt"

type EventEnvelope struct {
	Key     string
	Payload []byte
}

type EventCallback interface {
	ProcessEvent(key string, val interface{})
}

func (d *DistributedEventProcessor) streamNameForKey(key string) string {
	return fmt.Sprintf("%s:evt-str:%s", d.Namespace, key)
}

func (d *DistributedEventProcessor) processLockForKey(key string) string {
	return fmt.Sprintf("%s:pr-lk:%s", d.Namespace, key)
}
