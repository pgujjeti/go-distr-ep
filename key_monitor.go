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
	cdur := time.Duration(d.CleanupDur) * time.Second
	for {
		time.Sleep(cdur)
		log.Debug("Running cleanup ...")
		d.runKeyMonitor(cdur / 2)
	}
}

func (d *DistributedEventProcessor) runKeyMonitor(dur time.Duration) {
	ctx := context.Background()
	// Try to acquire monitor lock
	lock, err := d.locker.Obtain(ctx, d.monitorLockName, dur, nil)
	// Lock not acquired? return
	if err == redislock.ErrNotObtained {
		return
	}
	// TODO - limit the run of cleanup to the lock expiration
	defer lock.Release(ctx)
	// Check ZSet for scores < current-time
	c_time := time.Now().UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
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
		// Check the key-stream unprocessed size
		unprocessed_msg := d.unprocessedMessages(ctx, key)
		if unprocessed_msg > 0 {
			// Increment the time frame -> current-time + next-check
			d.refreshKeyMonitorExpiry(ctx, key)
			// Spin off the std event handler as a go-routine
			d.keyEventHandler(key)
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
	key string) int {
	xis, err := d.RedisClient.XInfoStreamFull(ctx, d.streamNameForKey(key), 1).Result()
	if err != nil {
		// TODO error handling
		return 0
	}
	var xg *redis.XInfoStreamGroup
	for _, xgl := range xis.Groups {
		if xg.Name == d.groupName {
			// Found the group
			xg = &xgl
			break
		}
	}
	if xg != nil &&
		(xis.LastGeneratedID != xg.LastDeliveredID || xg.PelCount > 0) {
		// group's last delivered id is lagging or group's pending count > 0
		return 1
	}
	return 0
}

func (d *DistributedEventProcessor) refreshKeyMonitorExpiry(ctx context.Context,
	key string) {
	// TODO - adding with an expiry of 2 * LockTTL, check on this logic
	exp_time := time.Now().Add(d.LockTTL * 2).UnixMilli()
	d.RedisClient.ZAdd(ctx, d.monitorZset,
		&redis.Z{Score: float64(exp_time), Member: key})
}
