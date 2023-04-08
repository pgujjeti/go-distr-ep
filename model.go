package distr_ep

import (
	"fmt"
	"time"
)

type EventCallback interface {
	// returns true if this key processing is completed
	ProcessEvent(key string, val interface{}, start bool) bool
}

type DistrEvent struct {
	Key string
	Val interface{}
}

const (
	REDIS_POS_LEFT  = "LEFT"
	REDIS_POS_RIGHT = "RIGHT"
)

func (d *DistributedEventProcessor) listNameForKey(key string) string {
	return fmt.Sprintf("dep:%s:key-el:%s", d.Namespace, key)
}

func (d *DistributedEventProcessor) processLockForKey(key string) string {
	return fmt.Sprintf("dep:%s:key-lk:%s", d.Namespace, key)
}

func (d *DistributedEventProcessor) processorSetKey(consumerId string) string {
	return fmt.Sprintf("dep:%s:pk-active:%s", d.Namespace, consumerId)
}

func (d *DistributedEventProcessor) processorEventsListName(consumerId string) string {
	return fmt.Sprintf("dep:%s:p-evts:%s", d.Namespace, consumerId)
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
