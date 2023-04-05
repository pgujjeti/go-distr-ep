package distr_ep

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
)

var (
	ErrTimeout = errors.New("op timeout")
)

func (d *DistributedEventProcessor) runEvent(e *DistrEvent) error {
	// add the event into key-specific {NS}:events:{key} entity
	ctx := context.Background()
	key := e.Key
	ln := d.listNameForKey(key)
	// add to Pending Keys list, if its a start event
	// TODO - optimize so that no explicit start is required
	if e.Start {
		// atomic op, along with insertion into key's event list?
		d.keyProcessor.pendingKey(e.Key)
	}
	// Add new events to the end of the list
	r, err := d.RedisClient.RPush(ctx, ln, e.Val).Result()
	dlog.Debugf("%s : add event for processing result: %v, %v", key, r, err)
	if err != nil {
		dlog.Warnf("%s : could not add event for processing: %v", key, err)
		return err
	}
	// renew List expiry
	d.RedisClient.Expire(ctx, ln, LIST_TTL)
	return nil
}

// tries to acquire key's lock: if successful, proceeds with processing the messages
// from the key's LIST
func (d *DistributedEventProcessor) keyEventHandler(ctx context.Context, key string) {
	// Retry once with a time limit of 100 ms
	// This is designed to address a potential race condition where another currently
	// processing client/thread might have completed message processing loop and
	// is about to release the key-lock
	// Obtain EVENT_LOCK to {NS}:proc-lock:{key}, with TTL
	pl_key := d.processLockForKey(key)
	lock := d.locker.NewMutex(pl_key,
		redsync.WithExpiry(d.LockTTL),
		redsync.WithRetryDelay(LOCK_RETRY_DUR),
		redsync.WithTries(1),
	)
	// If lock cant be obtained, return
	if err := lock.LockContext(ctx); err != nil {
		dlog.Infof("Client %s couldnt obtain lock %s for key %s: %v",
			d.consumerId, pl_key, key, err)
		return
	}
	// add this key to this processor's active list
	d.keyProcessor.addKeyToProcessor(ctx, key)
	// Kick off the event handler as a go-routine
	go d.runKeyProcessor(key, lock, ctx)
}

// runs a go-routine to process the given key
func (d *DistributedEventProcessor) runKeyProcessor(key string,
	lock *redsync.Mutex, ctx context.Context) {
	ln := d.listNameForKey(key)
	// Release key's lock before returning
	defer func() {
		// unlock the key
		// TODO PKD : check this context; might be invalid
		lock.UnlockContext(ctx)
	}()
	// Start consuming messages from the {NS}:evt-str:{key} stream
	completed := false
	start := true
	for {
		// extend lock before blocking for events
		lock.ExtendContext(ctx)
		// Seek the first event (event is removed post-processing)
		msg, err := d.fetchNextEvent(ctx, ln)
		if ctx.Err() != nil {
			dlog.Infof("%s : context cancelled for key %s: %v", d.consumerId, key, err)
			break
		}
		if err == redis.Nil {
			completed = true
			dlog.Infof("%s : no items to process for key %s from %s", d.consumerId, key, ln)
			break
		}
		if err != nil {
			completed = true
			dlog.Errorf("%s : couldnt fetch elements from %s: %v",
				key, ln, err)
			break
		}
		ejob := &eventProcessorJob{
			eventProcessor: d,
			key:            key,
			val:            msg,
			start:          start,
		}
		runProtectedJobWithLock(lock, d.LockTTL, ejob)
		d.markEventProcessed(ctx, ln, msg)
		start = false
		// stop the loop when processing is completed
		if ejob.completed {
			completed = true
			dlog.Infof("%s : key processing completed by processor %s", key, d.consumerId)
			break
		}
	}
	if completed {
		dlog.Infof("%s : cleaning up key %s", d.consumerId, key)
		// delete the key event list - SKIP
		// d.RedisClient.Del(ctx, ln)
		// remove the key from processor list
		d.keyProcessor.removeKeyForProcessor(ctx, key)
	}

}

func (d *DistributedEventProcessor) fetchNextEvent(ctx context.Context,
	ln string) (string, error) {
	dlog.Debugf("fetching an item from %s", ln)
	// If atleast-once semantics are set, peek the message. The message shall be
	// popped at the end of event processing in markEventProcessed()
	if d.AtLeastOnce {
		return d.peekElement(ctx, ln)
	}
	return d.popElement(ctx, ln)
}

func (d *DistributedEventProcessor) popElement(ctx context.Context,
	ln string) (string, error) {
	for t := time.Duration(0); t < d.EventPollTimeout; {
		r, err := d.RedisClient.LPop(ctx, ln).Result()
		switch err {
		case nil:
			return r, nil
		case redis.Nil:
		default:
			return "", err
		}
		t += d.eventPollDuration
		time.Sleep(d.eventPollDuration)
	}
	return "", ErrTimeout
	// TODO BLOCKING implementation
	// r, err := d.RedisClient.BLPop(ctx, d.EventPollTimeout, ln).Result()
	// if err != nil {
	// 	return "", err
	// }
	// return r[1], nil
}

func (d *DistributedEventProcessor) peekElement(ctx context.Context,
	ln string) (string, error) {
	for t := time.Duration(0); t < d.EventPollTimeout; {
		r, err := d.RedisClient.LIndex(ctx, ln, 0).Result()
		switch err {
		case nil:
			return r, nil
		case redis.Nil:
		default:
			return "", err
		}
		t += d.eventPollDuration
		time.Sleep(d.eventPollDuration)
	}
	return "", ErrTimeout
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
	start          bool
	completed      bool
}

func (e *eventProcessorJob) runJob(ch chan bool) {
	// indicate that polling completed at the end of the routine
	defer channelDone(ch, true)
	defer timeExecution(time.Now(), fmt.Sprintf("%s:event", e.key))
	d := e.eventProcessor
	// Invoke process event callback
	e.completed = d.Callback.ProcessEvent(e.key, e.val, e.start)
}
