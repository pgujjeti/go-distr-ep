package distr_ep

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type EventCallback interface {
	ProcessEvent(key string, val interface{})
}

func (d *DistributedEventProcessor) listNameForKey(key string) string {
	return fmt.Sprintf("%s:evt-ls:%s", d.Namespace, key)
}

func (d *DistributedEventProcessor) processLockForKey(key string) string {
	return fmt.Sprintf("%s:pr-lk:%s", d.Namespace, key)
}

func timeExecution(start time.Time, label string) {
	dur := time.Since(start)
	log.Debugf("%s: execution time (ns): %v", label, dur.Nanoseconds())
}
