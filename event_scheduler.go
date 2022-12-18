package distr_ep

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goccy/go-json"
	"github.com/rs/xid"
)

// Wrapper for schedule poll job - implements ProtectedJobRunner interface
type eventScheduler struct {
	enabled  bool
	zsetKey  string
	hsetKey  string
	lockName string
	dur      time.Duration
	d        *DistributedEventProcessor
	pollTask *pollingTask
}

func (m *eventScheduler) start() {
	if !m.enabled {
		dlog.Infof("%s : Event Scheduling is disabled", m.d.consumerId)
		return
	}
	m.pollTask = &pollingTask{
		dur: m.dur,
		job: m,
	}
	m.pollTask.start()
}

func (m *eventScheduler) stop() {
	if !m.enabled {
		return
	}
	dlog.Infof("%s : stopping scheduler", m.d.consumerId)
	m.pollTask.stop()
}

func (m *eventScheduler) runPeriodicJob() {
	dlog.Debugf("consumer %s : checking for scheduled jobs...", m.d.consumerId)
	runProtectedJob(m.d.locker, m.lockName, m.dur, m)
}

func (m *eventScheduler) pollEnded() {
	dlog.Warnf("%s : stopped key monitor", m.d.consumerId)
}

func (m *eventScheduler) scheduleEvent(e *DistrEvent,
	delay time.Duration) error {
	d := m.d
	if !m.enabled {
		dlog.Warnf("%s : event scheduling is disabled", d.consumerId)
		return errors.New("scheduling disabled")
	}
	// add event to the scheduler zset
	ctx := context.Background()
	key := e.Key
	evt_key := d.createSchEvtKey(key)
	// Add key-val to HashSet
	var evt_s []byte
	var err error
	if evt_s, err = json.Marshal(e); err != nil {
		dlog.Warnf("%s : could not marshal event for scheduling: %v", key, err)
		return err
	}
	if err := d.RedisClient.HSet(ctx, m.hsetKey, evt_key, evt_s).Err(); err != nil {
		dlog.Warnf("%s : could not schedule event for processing: %v", key, err)
		return err
	}
	// Add key-score to Zset
	exp_time := time.Now().Add(delay).UnixMilli()
	z := &redis.Z{Score: float64(exp_time), Member: evt_key}
	if err := d.RedisClient.ZAdd(ctx, m.zsetKey, z).Err(); err != nil {
		dlog.Warnf("%s : could not schedule event for processing: %v", key, err)
		// Delete the entry from hset
		d.RedisClient.HDel(ctx, m.hsetKey, evt_key)
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

func (s *eventScheduler) runJob(ch chan bool) {
	// indicate that polling completed at the end of the routine
	defer channelDone(ch, true)
	s.pollScheduledEvents(context.Background())
}

func (s *eventScheduler) pollScheduledEvents(ctx context.Context) {
	d := s.d
	// Check ZSet for scores < current-time
	c_time := time.Now().UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
	dlog.Infof("consumer %s : checking for events before %s", d.consumerId, c_time_str)
	zrb := &redis.ZRangeBy{
		Max: c_time_str,
	}
	ra, err := d.RedisClient.ZRangeByScoreWithScores(ctx, s.zsetKey, zrb).Result()
	if err != nil {
		dlog.Warnf("consumer %s : could not run zrangebyscore on scheduler zset: %v",
			d.consumerId, err)
		return
	}
	for _, rz := range ra {
		evt_key := rz.Member.(string)
		dlog.Debugf("checking key %s with expiry %s", evt_key, rz.Score)
		s.handleCurrentEvent(ctx, evt_key)
	}
}

func (s *eventScheduler) handleCurrentEvent(ctx context.Context, evt_key string) {
	d := s.d
	// run event & delete event
	defer func() {
		// Delete event key from Hset & Zset
		d.RedisClient.HDel(ctx, s.hsetKey, evt_key).Result()
		d.RedisClient.ZRem(ctx, s.zsetKey, evt_key)
	}()
	val, err := d.RedisClient.HGet(ctx, s.hsetKey, evt_key).Result()
	if err != nil {
		dlog.Warnf("%s : could not find value in hash set %s: %v", evt_key,
			s.hsetKey, err)
		return
	}
	var e DistrEvent
	if err := json.Unmarshal([]byte(val), &e); err == nil {
		// submit event to run
		d.runEvent(&e)
	} else {
		// Should never happen
		dlog.Warnf("%s : invalid key found: %v", evt_key, err)
	}
}
