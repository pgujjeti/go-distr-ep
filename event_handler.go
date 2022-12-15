package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
)

func (d *DistributedEventProcessor) runEvent(e *DistrEvent) error {
	// add the event into key-specific {NS}:events:{key} entity
	ctx := context.Background()
	key := e.Key
	ln := d.listNameForKey(key)
	// add to Pending Keys list, if its a start event
	if e.Start {
		// TODO PKD - atomic op (along with insertion into key's event list)
		d.pendingKey(e)
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

func (d *DistributedEventProcessor) pendingKey(e *DistrEvent) error {
	key := e.Key
	ln := d.pKeyList
	ctx := context.Background()
	// add the key to pending keys
	r, err := d.RedisClient.RPush(ctx, ln, key).Result()
	dlog.Debugf("%s : add key to pending keys result: %v, %v", key, r, err)
	if err != nil {
		dlog.Warnf("%s : could not add key to pending keys: %v", key, err)
		return err
	}
	return nil
}

// consume and run pending keys
func (d *DistributedEventProcessor) pendingKeysConsumer() {
	// run the consumer loop
	for {
		ctx := context.Background()
		// TODO PKD - use BLMove with a temp processing list
		// key, err := d.RedisClient.BLMove(ctx, d.pKeyList, d.pkProcList, "LEFT", "RIGHT", 0).Result()
		key, err := d.RedisClient.BLPop(ctx, 0, d.pKeyList).Result()
		if err == redis.Nil || len(key) == 0 {
			dlog.Debugf("%s : no pending keys; retrying", d.consumerId)
			continue
		}
		if err != nil {
			dlog.Warnf("%s : fetching pending keys failed.. retrying: %v", d.consumerId, err)
			continue
		}
		d.keyEventHandler(ctx, key[0])
	}
}

func (d *DistributedEventProcessor) keyEventHandler(ctx context.Context, key string) {
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
		redsync.WithRetryDelay(LOCK_RETRY_DUR),
		redsync.WithTries(1),
	)
	// If lock cant be obtained, return
	if err := lock.LockContext(ctx); err != nil {
		dlog.Debugf("Client %s couldnt obtain lock for key %s: %v", d.consumerId, key, err)
		return
	}
	// Defer lock release
	defer lock.UnlockContext(ctx)
	// add this key to this processor's active list
	d.addKeyToProcessor(ctx, key)
	// Start consuming messages from the {NS}:evt-str:{key} stream
	ln := d.listNameForKey(key)
	for {
		// Seek the first event (event is removed post-processing)
		// TODO PKD - consume messages
		// renew the key before every turn
		// stop the loop when processing is completed
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
	}
}

func (d *DistributedEventProcessor) fetchNextEvent(ctx context.Context,
	ln string) (string, error) {
	// If atleast-once semantics are set, peek the message. The message shall be
	// popped at the end of event processing in markEventProcessed()
	if d.AtLeastOnce {
		return d.RedisClient.LIndex(ctx, ln, 0).Result()
	}
	// TODO PKD - use BLMove
	r, err := d.RedisClient.BLPop(ctx, d.EventPollTimeout, ln).Result()
	if err != nil {
		return "", err
	}
	return r[0], nil
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
