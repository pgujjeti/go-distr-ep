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

	REDIS_NS = "dep:%s:"
	// https://redis.io/docs/reference/cluster-spec/#hash-tags
	// REDIS_PK_HASH_PREFIX      = "{dep:%s:pk-}"
	REDIS_KEY_NODE_HSET = REDIS_NS + "kn-s"
	REDIS_KEY_LIST      = REDIS_NS + "k-el:%s"
	REDIS_KEY_LOCK      = REDIS_NS + "k-lk:%s"
	REDIS_PROC_ACTIVE   = REDIS_NS + "p-ac:%s"
	REDIS_PROC_EVTS     = REDIS_NS + "p-ev:%s"
	REDIS_MON_ZSET      = REDIS_NS + "m-zs"
	REDIS_MON_ZSET_LK   = REDIS_NS + "m-lk"
	REDIS_SCH_ZSET      = REDIS_NS + "s-zs"
	REDIS_SCH_ZSET_LK   = REDIS_NS + "s-lk"
	REDIS_SCH_HASH      = REDIS_NS + "s-hs"
)

func (d *DistributedEventProcessor) listNameForKey(key string) string {
	return fmt.Sprintf(REDIS_KEY_LIST, d.Namespace, key)
}

func (d *DistributedEventProcessor) lockNameForKey(key string) string {
	return fmt.Sprintf(REDIS_KEY_LOCK, d.Namespace, key)
}

func (d *DistributedEventProcessor) processorSetKey(consumerId string) string {
	return fmt.Sprintf(REDIS_KEY_LOCK, d.Namespace, consumerId)
}

func (d *DistributedEventProcessor) processorEventsListName(consumerId string) string {
	return fmt.Sprintf(REDIS_PROC_EVTS, d.Namespace, consumerId)
}

func (d *DistributedEventProcessor) monZsetKeyName() string {
	return fmt.Sprintf(REDIS_MON_ZSET, d.Namespace)
}

func (d *DistributedEventProcessor) monZsetLockName() string {
	return fmt.Sprintf(REDIS_MON_ZSET_LK, d.Namespace)
}

func (d *DistributedEventProcessor) schZsetKeyName() string {
	return fmt.Sprintf(REDIS_SCH_ZSET, d.Namespace)
}

func (d *DistributedEventProcessor) schZsetLockName() string {
	return fmt.Sprintf(REDIS_SCH_ZSET_LK, d.Namespace)
}

func (d *DistributedEventProcessor) schHashKeyName() string {
	return fmt.Sprintf(REDIS_SCH_HASH, d.Namespace)
}

func (d *DistributedEventProcessor) keyNodeHashName() string {
	return fmt.Sprintf(REDIS_KEY_NODE_HSET, d.Namespace)
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
