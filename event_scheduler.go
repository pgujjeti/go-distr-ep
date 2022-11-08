package distr_ep

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"
)

func (d *DistributedEventProcessor) scheduleEvent(key string, val interface{},
	delay time.Duration) error {
	if !d.Scheduling {
		dlog.Warn("%s : event scheduling is disabled", d.consumerId)
		return errors.New("scheduling disabled")
	}
	// add event to the scheduler zset
	ctx := context.Background()
	evt_key := d.createSchEvtKey(key)
	// Add key-val to HashSet
	if err := d.RedisClient.HSet(ctx, d.schedulerHset, evt_key, val).Err(); err != nil {
		dlog.Warnf("%s : could not schedule event for processing: %v", key, err)
		return err
	}
	// Add key-score to Zset
	exp_time := time.Now().Add(delay).UnixMilli()
	z := &redis.Z{Score: float64(exp_time), Member: evt_key}
	if err := d.RedisClient.ZAdd(ctx, d.schedulerZset, z).Err(); err != nil {
		dlog.Warnf("%s : could not schedule event for processing: %v", key, err)
		// Delete the entry from hset
		d.RedisClient.HDel(ctx, d.schedulerHset, evt_key)
		return err
	}
	dlog.Debugf("%s : added scheduled event with composite key %s expiring at %v",
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
	dlog.Debugf("Extracted key %s from composite key %s", key, evt_key)
	return key, nil
}

func (d *DistributedEventProcessor) eventScheduler() {
	if !d.Scheduling {
		dlog.Warn("%s : Event Scheduling is disabled", d.consumerId)
		return
	}
	cdur := DEFAULT_SCHEDULE_DUR
	ticker := time.NewTicker(cdur)
	for range ticker.C {
		dlog.Debug("consumer %s : checking for scheduled jobs ...", d.consumerId)
		spj := &schedulePollJob{eventProcessor: d}
		runProtectedJob(d.locker, d.schedulerLock, cdur, spj)
	}
}

// Wrapper for schedule poll job - implements ProtectedJobRunner interface
type schedulePollJob struct {
	eventProcessor *DistributedEventProcessor
}

func (s *schedulePollJob) runJob(ch chan bool) {
	// indicate that polling completed at the end of the routine
	defer channelDone(ch, true)
	s.eventProcessor.pollScheduledEvents(context.Background())
}

func (d *DistributedEventProcessor) pollScheduledEvents(ctx context.Context) {
	// Check ZSet for scores < current-time
	c_time := time.Now().UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
	dlog.Infof("consumer %s : checking for events before %s", d.consumerId, c_time_str)
	zrb := &redis.ZRangeBy{
		Max: c_time_str,
	}
	ra, err := d.RedisClient.ZRangeByScoreWithScores(ctx, d.schedulerZset, zrb).Result()
	if err != nil {
		dlog.Warnf("consumer %s : could not run zrangebyscore on scheduler zset: %v",
			d.consumerId, err)
		return
	}
	for _, rz := range ra {
		evt_key := rz.Member.(string)
		dlog.Debugf("checking key %s with expiry %s", evt_key, rz.Score)
		d.handleCurrentEvent(ctx, evt_key)
	}
}

func (d *DistributedEventProcessor) handleCurrentEvent(ctx context.Context, evt_key string) {
	// run event & delete event
	val, err := d.RedisClient.HGet(ctx, d.schedulerHset, evt_key).Result()
	if err != nil {
		dlog.Warnf("%s : could not find value in hash set %s: %v", evt_key,
			d.schedulerHset, err)
	} else {
		if key, err := d.extractKeyFromSchEvtKey(evt_key); err == nil {
			// submit event to run
			d.runEvent(key, val)
		} else {
			// Should never happen
			dlog.Warnf("%s : invalid key found: %v", evt_key, err)
		}
	}
	// Delete event key from Hset & Zset
	d.RedisClient.HDel(ctx, d.schedulerHset, evt_key).Result()
	d.RedisClient.ZRem(ctx, d.schedulerZset, evt_key)
}
