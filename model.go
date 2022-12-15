package distr_ep

import (
	"fmt"
	"time"
)

type EventCallback interface {
	// invoked when start processing
	StartProcessing(key string)
	ProcessEvent(key string, val interface{})
}

type DistrEvent struct {
	Key   string
	Val   interface{}
	Start bool
}

func (d *DistributedEventProcessor) listNameForKey(key string) string {
	return fmt.Sprintf("%s:evt-ls:%s", d.Namespace, key)
}

func (d *DistributedEventProcessor) processLockForKey(key string) string {
	return fmt.Sprintf("%s:pr-lk:%s", d.Namespace, key)
}

// utility functions

// use to time execution of a function, block, etc
func timeExecution(start time.Time, label string) {
	dur := time.Since(start)
	dlog.Debugf("%s: execution time (ns): %v", label, dur.Nanoseconds())
}

// use to indicate completion of a routine
func channelDone(ch chan bool, val bool) {
	ch <- val
}
