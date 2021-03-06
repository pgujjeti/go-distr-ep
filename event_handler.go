package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
)

func (d *DistributedEventProcessor) runEvent(key string, val interface{}) error {
	// add the event into key-specific {NS}:events:{key} entity
	ctx := context.Background()
	ln := d.listNameForKey(key)
	// Add new events to the end of the list
	r, err := d.RedisClient.RPush(ctx, ln, val).Result()
	dlog.Debugf("%s : add event for processing result: %v, %v", key, r, err)
	if err != nil {
		dlog.Warnf("%s : could not add event for processing: %v", key, err)
		return err
	}
	// renew List expiry
	d.RedisClient.Expire(ctx, ln, DEFAULT_LIST_TTL)
	d.keyEventHandler(ctx, key)
	return nil
}

func (d *DistributedEventProcessor) keyEventHandler(ctx context.Context, key string) {
	// add to the monitor ZSET {NS}:k-monitor
	d.refreshKeyMonitorExpiry(ctx, key)
	// Kick off the event handler as a go-routine
	go d.runKeyProcessor(key)
}

// runs a go-routine to process the given key
// tries to acquire key's lock: if successful, proceeds with processing the messages
// from the key's LIST
func (d *DistributedEventProcessor) runKeyProcessor(key string) {
	ctx := context.Background()
	// Retry once with a time limit of 100 ms
	// This is designed to address a potential race condition where another currently
	// processing client/thread might have completed message processing loop and
	// is about to release the key-lock
	// Obtain EVENT_LOCK to {NS}:proc-lock:{key}, with TTL
	lock := d.locker.NewMutex(d.processLockForKey(key),
		redsync.WithExpiry(d.LockTTL),
		redsync.WithRetryDelay(DEFAULT_LOCK_RETRY_DUR),
		redsync.WithTries(1),
	)
	// If lock cant be obtained, return
	if err := lock.LockContext(ctx); err != nil {
		dlog.Debugf("Client %s couldnt obtain lock for key %s: %v", d.consumerId, key, err)
		return
	}
	// Defer lock release
	defer lock.UnlockContext(ctx)
	// Start consuming messages from the {NS}:evt-str:{key} stream
	ln := d.listNameForKey(key)
	for {
		len, err := d.RedisClient.LLen(ctx, ln).Result()
		dlog.Debugf("%s : %v elements to process in %s: %v", key, len, ln, err)
		if len == 0 || err != nil {
			dlog.Warnf("%s : no more messages to process: %v", key, err)
			break
		}
		// Seek the first event (event is removed post-processing)
		msg, err := d.fetchNextEvent(ctx, ln)
		if err != nil {
			dlog.Infof("%s : couldnt fetch the first element from %s: %v",
				key, ln, err)
			break
		}
		ejob := &eventProcessorJob{
			eventProcessor: d,
			key:            key,
			val:            msg,
		}
		runProtectedJobWithLock(lock, d.LockTTL, ejob)
		d.markEventProcessed(ctx, ln, msg)
		// Lock should be active. Discontinue, if it is not (circuit-breaker)
		if until := lock.Until(); time.Now().After(until) {
			// Lock expired!
			dlog.Errorf("%s : LOCK EXPIRE while processing message %s: ", key, msg)
			break
		}
	}
}

func (d *DistributedEventProcessor) fetchNextEvent(ctx context.Context,
	ln string) (string, error) {
	// If atleast-once semantics are set, peek the message. The message shall be
	// popped at the end of event processing in markEventProcessed()
	if d.AtLeastOnce {
		return d.RedisClient.LIndex(ctx, ln, 0).Result()
	}
	return d.RedisClient.LPop(ctx, ln).Result()
}

func (d *DistributedEventProcessor) markEventProcessed(ctx context.Context,
	ln string, msg string) error {
	dlog.Tracef("list [%s] : event processed : %s", ln, msg)
	var err error
	if d.AtLeastOnce {
		// mark message as processed, by popping the first event from the list
		_, err = d.RedisClient.LPop(ctx, ln).Result()
	}
	return err
}

// Wrapper for event processor job - implements ProtectedJobRunner interface
type eventProcessorJob struct {
	eventProcessor *DistributedEventProcessor
	key            string
	val            string
}

func (e *eventProcessorJob) runJob(ch chan bool) {
	// indicate that polling completed at the end of the routine
	defer channelDone(ch, true)
	defer timeExecution(time.Now(), fmt.Sprintf("%s:event", e.key))
	d := e.eventProcessor
	// Invoke process event callback
	d.Callback.ProcessEvent(e.key, e.val)
}
