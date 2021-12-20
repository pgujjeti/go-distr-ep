package distr_ep

import (
	"context"

	"github.com/bsm/redislock"
	log "github.com/sirupsen/logrus"
)

func (d *DistributedEventProcessor) keyEventHandler(key string) {
	ctx := context.Background()
	// Obtain EVENT_LOCK to {NS}:proc-lock:{key}, with TTL
	lock, err := d.locker.Obtain(ctx, d.processLockForKey(key),
		d.LockTTL, nil)
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
