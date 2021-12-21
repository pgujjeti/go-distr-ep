package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	log "github.com/sirupsen/logrus"
)

func (d *DistributedEventProcessor) runEvent(key string, val interface{}) error {
	// add the event into key-specific {NS}:events:{key} entity
	ctx := context.Background()
	ln := d.listNameForKey(key)
	// Add new events to the end of the list
	r, err := d.RedisClient.RPush(ctx, ln, val).Result()
	log.Debugf("%s : add event for processing result: %v, %v", key, r, err)
	if err != nil {
		log.Warnf("%s : could not add event for processing: %v", key, err)
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
	// Retry once with a time limit of 10 ms
	// This is designed to address a potential race condition where another currently
	// processing thread might have checked out of the message processing loop and
	// is about to release the key-lock
	retry := redislock.LimitRetry(redislock.LinearBackoff(DEFAULT_LOCK_RETRY_DUR), 1)
	// Obtain EVENT_LOCK to {NS}:proc-lock:{key}, with TTL
	lock, err := d.locker.Obtain(ctx, d.processLockForKey(key),
		d.LockTTL, &redislock.Options{RetryStrategy: retry})
	// If lock cant be obtained, return
	if err == redislock.ErrNotObtained {
		log.Debugf("Client %s couldnt obtain lock for key %s, exiting...", d.consumerId, key)
		return
	}
	// Defer lock release
	defer lock.Release(ctx)
	// Start consuming messages from the {NS}:evt-str:{key} stream
	ln := d.listNameForKey(key)
	for {
		len, err := d.RedisClient.LLen(ctx, ln).Result()
		log.Debugf("%s : %v elements to process in %s: %v", key, len, ln, err)
		if len == 0 || err != nil {
			log.Warnf("%s : no more messages to process: %v", key, err)
			break
		}
		// Seek the first event (event is removed post-processing)
		msg, err := d.RedisClient.LIndex(ctx, ln, 0).Result()
		if err != nil {
			log.Infof("%s : couldnt fetch the first element from %s: %v",
				key, ln, err)
			break
		}
		d.handleListEvent(key, msg, lock, ctx)
		// mark message as processed, by popping the first event from the list
		d.RedisClient.LPop(ctx, ln)
		// Lock should be active. Discontinue, if it is not (circuit-breaker)
		if ttl, err := lock.TTL(ctx); err != nil || ttl == 0 {
			// Lock expired!
			log.Errorf("%s : LOCK EXPIRE while processing message %s", key, msg)
			break
		}
	}
}

func (d *DistributedEventProcessor) handleListEvent(key string, msg string,
	lock *redislock.Lock, ctx context.Context) {
	defer timeExecution(time.Now(), fmt.Sprintf("%s : processing event", key))
	// renew lock's TTL
	lock.Refresh(ctx, d.LockTTL, nil)
	// process message synchronously
	// keep renewing the lock while the element is in process
	lock_ticker := time.NewTicker(d.LockTTL / 2)
	defer lock_ticker.Stop()
	ch_done := make(chan bool)
	go d.processEvent(key, msg, ch_done)
	for {
		select {
		case <-lock_ticker.C:
			log.Debugf("%s : renewing lock for %v", key, d.LockTTL)
			// renew lock's TTL
			lock.Refresh(ctx, d.LockTTL, nil)
		case <-ch_done:
			// processing done
			log.Debugf("%s : processed msg: %s", key, msg)
			lock_ticker.Stop()
			return
		}
	}
}

func (d *DistributedEventProcessor) processEvent(key string,
	val interface{}, ch chan bool) {
	// Invoke process event callback
	d.Callback.ProcessEvent(key, val)
	ch <- true
}
