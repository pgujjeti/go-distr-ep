package distr_ep

import (
	"context"

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
	// add to the monitor ZSET {NS}:k-monitor
	d.refreshKeyMonitorExpiry(ctx, key)
	// Kick off the event handler
	go d.keyEventHandler(key)
	return nil
}

func (d *DistributedEventProcessor) keyEventHandler(key string) {
	ctx := context.Background()
	// Retry once with a time limit of 10 ms
	// This is designed to address a potential race condition where another currently
	// processing thread might have checked out of the message processing loop and
	// is about to release the key-lock
	// TODO - validate this works
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
		// Seek the first element, without removing. Element is removed at the end of processing
		msg, err := d.RedisClient.LIndex(ctx, ln, 0).Result()
		if err != nil {
			log.Infof("%s : couldnt fetch the first element from %s: %v",
				key, ln, err)
			break
		}
		// renew lock's TTL
		lock.Refresh(ctx, d.LockTTL, nil)
		// process message synchronously
		d.processEvent(key, msg)
		// mark message as processed, by popping the first element from the list
		d.RedisClient.LPop(ctx, ln)
	}
}

func (d *DistributedEventProcessor) processEvent(key string, val interface{}) error {
	d.Callback.ProcessEvent(key, val)
	return nil
}
