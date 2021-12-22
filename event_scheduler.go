package distr_ep

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

func (d *DistributedEventProcessor) scheduleEvent(key string, val interface{},
	delay time.Duration) error {
	// add event to the scheduler zset
	ctx := context.Background()
	evt_key := d.createSchEvtKey(key)
	// Add key-val to HashSet
	d.RedisClient.HSet(ctx, d.schedulerHset, evt_key, val)
	// Add key-score to Zset
	exp_time := time.Now().Add(delay).UnixMilli()
	d.RedisClient.ZAdd(ctx, d.schedulerZset,
		&redis.Z{Score: float64(exp_time), Member: evt_key})
	log.Debugf("%s : added scheduled event with composite key %s expiring at %v",
		key, evt_key, exp_time)
	return nil
}

func (d *DistributedEventProcessor) createSchEvtKey(key string) string {
	uid := xid.New().String()
	return fmt.Sprintf("%s:%s:%s", uid, d.consumerId, key)
}

func (d *DistributedEventProcessor) extractKeyFromSchEvtKey(evt_key string) (string, error) {
	k_parts := strings.Split(evt_key, ":")
	if len(k_parts) != 3 {
		return "", errors.New("invalid key")
	}
	key := k_parts[2]
	log.Debugf("Extracted key %s from composite key %s", key, evt_key)
	return key, nil
}

func (d *DistributedEventProcessor) eventScheduler() {
	cdur := DEFAULT_SCHEDULE_DUR
	ticker := time.NewTicker(cdur)
	for {
		select {
		case <-ticker.C:
			log.Debug("consumer %s : checking for scheduled jobs ...", d.consumerId)
			d.runSchedulerJob(cdur)
		}
	}
}

// TODO - make this reusable code (scheduler, monitor, event-processor)
func (d *DistributedEventProcessor) runSchedulerJob(dur time.Duration) {
	defer timeExecution(time.Now(), "scheduler-run")
	ctx := context.Background()
	// Try to acquire scheduler lock
	lock, err := d.locker.Obtain(ctx, d.schedulerLock, dur, nil)
	// Lock not acquired? return
	if err == redislock.ErrNotObtained {
		log.Debugf("consumer %s : could not obtain scheduler lock", d.consumerId)
		return
	}
	defer lock.Release(ctx)
	// Refresh the lock while the scheduler runs with a ticker
	// running at 1/2 the lock duration
	ticker := time.NewTicker(dur / 2)
	defer ticker.Stop()
	// channel to indicate when the checkKeys routine is completed
	ch_done := make(chan bool)
	go d.pollScheduledEvents(ctx, dur, ch_done)
	for {
		select {
		case <-ticker.C:
			// renew lock
			log.Debugf("consumer %s : schedule poller still running", d.consumerId)
			lock.Refresh(ctx, dur, nil)
		case <-ch_done:
			// Job complete
			ticker.Stop()
			log.Debugf("consumer %s : schedule poller COMPLETED", d.consumerId)
			return
		}
	}
}

func (d *DistributedEventProcessor) pollScheduledEvents(ctx context.Context,
	dur time.Duration, ch chan bool) {
	// indicates that polling completed
	defer channelDone(ch, true)
	// Check ZSet for scores < current-time
	c_time := time.Now().UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
	log.Infof("consumer %s : checking for events before %s", d.consumerId, c_time_str)
	zrb := &redis.ZRangeBy{
		Max: c_time_str,
	}
	ra, err := d.RedisClient.ZRangeByScoreWithScores(ctx, d.schedulerZset, zrb).Result()
	if err != nil {
		log.Warnf("consumer %s : could not run zrangebyscore on scheduler zset: %v",
			d.consumerId, err)
		return
	}
	for _, rz := range ra {
		evt_key := rz.Member.(string)
		log.Debugf("checking key %s with expiry %s", evt_key, rz.Score)
		d.handleCurrentEvent(ctx, evt_key)
	}
}

func (d *DistributedEventProcessor) handleCurrentEvent(ctx context.Context, evt_key string) {
	// run event & delete event
	val, err := d.RedisClient.HGet(ctx, d.schedulerHset, evt_key).Result()
	if err != nil {
		log.Warnf("%s : could not find value in hash set %s: %v", evt_key,
			d.schedulerHset, err)
	} else {
		if key, err := d.extractKeyFromSchEvtKey(evt_key); err == nil {
			// submit event to run
			d.runEvent(key, val)
		} else {
			// Should never happen
			log.Warnf("%s : invalid key found: %v", evt_key, err)
		}
	}
	// Delete event key from Hset & Zset
	d.RedisClient.HDel(ctx, d.schedulerHset, evt_key).Result()
	d.RedisClient.ZRem(ctx, d.schedulerZset, evt_key)
}
