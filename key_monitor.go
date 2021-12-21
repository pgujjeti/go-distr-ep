package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func (d *DistributedEventProcessor) monitorKeys() {
	cdur := d.CleanupDur
	ticker := time.NewTicker(cdur)
	for {
		select {
		case <-ticker.C:
			log.Debug("consumer %s : running cleanup ...", d.consumerId)
			d.runKeyMonitor(cdur)
		}
	}
}

func (d *DistributedEventProcessor) runKeyMonitor(dur time.Duration) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(dur))
	// TODO - limit the duration of cleanup to be under the lock expiration
	// validate this logic
	defer cancel()
	// Try to acquire monitor lock
	lock, err := d.locker.Obtain(ctx, d.monitorLock, dur, nil)
	// Lock not acquired? return
	if err == redislock.ErrNotObtained {
		log.Debugf("consumer %s : could not obtain monitor lock", d.consumerId)
		return
	}
	defer lock.Release(ctx)
	// Check ZSet for scores < current-time
	c_time := time.Now().UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
	log.Infof("consumer %s : checking for keys before %s", d.consumerId, c_time_str)
	zrb := &redis.ZRangeBy{
		Max: c_time_str,
	}
	ra, err := d.RedisClient.ZRangeByScoreWithScores(ctx, d.monitorZset, zrb).Result()
	if err != nil {
		return
	}
	for _, rz := range ra {
		key := rz.Member.(string)
		log.Debugf("checking key %s with expiry %s", key, rz.Score)
		// Check the key-stream for unprocessed/in-process msgs
		unprocessed_msg := d.unprocessedMessages(ctx, key)
		if unprocessed_msg {
			// Renews the check-TTL and launches the key-processor
			d.keyEventHandler(ctx, key)
		} else {
			log.Debugf("key %s shall be deleted", key)
			// no pending msgs - key will cleaned up after the run
			// delete the stream?
		}
	}
	// Delete all items with score < current-time
	d.RedisClient.ZRemRangeByScore(ctx, d.monitorZset, "0", c_time_str)
}

func (d *DistributedEventProcessor) unprocessedMessages(ctx context.Context,
	key string) bool {
	ln := d.listNameForKey(key)
	len, err := d.RedisClient.LLen(ctx, ln).Result()
	log.Tracef("%s : %v unprocessed messages in LIST %s", key, len, ln)
	if err != nil {
		// error handling
		log.Warnf("%s : couldnt find the length of LIST %s", key, ln)
		return false
	}
	return len > 0
}

func (d *DistributedEventProcessor) refreshKeyMonitorExpiry(ctx context.Context,
	key string) {
	// add this key with an expiry of 2 * LockTTL
	exp_time := time.Now().Add(d.LockTTL * 2).UnixMilli()
	d.RedisClient.ZAdd(ctx, d.monitorZset,
		&redis.Z{Score: float64(exp_time), Member: key})
}
