package distr_ep

import (
	"context"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func (d *DistributedEventProcessor) keyEventHandler(key string) {
	ctx := context.Background()
	// Obtain EVENT_LOCK to {NS}:proc-lock:{key}, with TTL
	lock, err := d.locker.Obtain(ctx, d.processLockForKey(key),
		d.LockTTL, nil)
	// If lock cant be obtained, return
	if err == redislock.ErrNotObtained {
		return
	}
	// Defer lock release
	defer lock.Release(ctx)
	// Start consuming messages from the {NS}:events:{key} stream
	sn := d.streamNameForKey(key)
	for {
		rga := &redis.XReadGroupArgs{
			Group:    d.groupName,
			Consumer: d.consumerId,
			Streams:  []string{sn, ">"},
			Count:    2,
			Block:    d.LockTTL / 2,
			NoAck:    false,
		}
		xs, err := d.RedisClient.XReadGroup(ctx, rga).Result()
		if err != nil || len(xs) == 0 || len(xs[0].Messages) == 0 {
			log.Debugf("%s : no more messages to process", key)
			break
		}
		for _, msg := range xs[0].Messages {
			// when message is received, renew lock's TTL (to account for wait time)
			lock.Refresh(ctx, d.LockTTL, nil)
			// process message synchronously
			d.processEvent(key, msg.Values["val"])
			// mark message as processed
			d.RedisClient.XAck(ctx, sn, d.groupName, msg.ID)
		}
	}
}

func (d *DistributedEventProcessor) processEvent(key string, val interface{}) error {
	d.Callback.ProcessEvent(key, val)
	return nil
}
