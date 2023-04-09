package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Wrapper for monitor - implements ProtectedJobRunner interface
type keyMonitor struct {
	zsetKey  string
	lockName string
	dur      time.Duration
	d        *DistributedEventProcessor
	pollTask *pollingTask
}

func (m *keyMonitor) start() {
	m.pollTask = &pollingTask{
		dur: m.dur,
		job: m,
	}
	m.pollTask.start()
}

func (m *keyMonitor) stop() {
	m.pollTask.stop()
}

func (m *keyMonitor) runPeriodicJob() {
	m.checkinProcessor()
	dlog.Debugf("consumer %s : running cleanup ...", m.d.consumerId)
	runProtectedJob(m.d.locker, m.lockName, m.dur, m)
}

func (m *keyMonitor) pollEnded() {
	dlog.Warnf("%s : stopped key monitor", m.d.consumerId)
}

func (m *keyMonitor) checkinProcessor() {
	d := m.d
	// update this processor's timestamp in ZSET
	ctx := context.Background()
	z := &redis.Z{Score: float64(time.Now().UnixMilli()), Member: d.consumerId}
	if err := d.RedisClient.ZAdd(ctx, m.zsetKey, z).Err(); err != nil {
		// terminate the processor?
		dlog.Errorf("%s : could not check-in the processor", d.consumerId)
	}
}

func (m *keyMonitor) runJob(ch chan bool) {
	// indicate that polling completed at the end of the routine
	defer channelDone(ch, true)
	m.checkProcessors(context.Background())
}

// Run the checker to see if a processor is dead
func (m *keyMonitor) checkProcessors(ctx context.Context) {
	d := m.d
	// Check ZSet for scores < current-time
	c_time := time.Now().Add(time.Duration(-3) * d.CleanupDur).UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
	dlog.Infof("consumer %s : checking for processors before %s", d.consumerId, c_time_str)
	zrb := &redis.ZRangeBy{
		Max: c_time_str,
	}
	ra, err := d.RedisClient.ZRangeByScoreWithScores(ctx, m.zsetKey, zrb).Result()
	if err != nil {
		dlog.Warnf("consumer %s : could not run zrangebyscore %v", d.consumerId, err)
		return
	}
	for _, rz := range ra {
		processor := rz.Member.(string)
		dlog.Warnf("consumer %s expired with last updated time %v", processor, rz.Score)
		if err := d.keyProcessor.deleteProcessor(processor); err != nil {
			continue
		}
		// remove the processor
		d.RedisClient.ZRem(ctx, m.zsetKey, processor)
	}
}
