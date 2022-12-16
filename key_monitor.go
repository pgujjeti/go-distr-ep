package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func (d *DistributedEventProcessor) monitorKeys() {
	cdur := d.CleanupDur
	ticker := time.NewTicker(cdur)
	spj := &monitorJob{eventProcessor: d}
	for range ticker.C {
		d.checkinProcessor()
		dlog.Debugf("consumer %s : running cleanup ...", d.consumerId)
		runProtectedJob(d.locker, d.monitorLock, cdur, spj)
	}
}

func (d *DistributedEventProcessor) checkinProcessor() {
	// update this processor's timestamp in ZSET
	ctx := context.Background()
	z := &redis.Z{Score: float64(time.Now().UnixMilli()), Member: d.consumerId}
	if err := d.RedisClient.ZAdd(ctx, d.monitorZset, z).Err(); err != nil {
		// terminate the processor?
		dlog.Errorf("%s : could not checkin the processor", d.consumerId)
	}
}

// Wrapper for monitor - implements ProtectedJobRunner interface
type monitorJob struct {
	eventProcessor *DistributedEventProcessor
}

func (m *monitorJob) runJob(ch chan bool) {
	// indicate that polling completed at the end of the routine
	defer channelDone(ch, true)
	m.eventProcessor.checkProcessors(context.Background())
}

// Run the checker to see if a processor is dead
func (d *DistributedEventProcessor) checkProcessors(ctx context.Context) {
	// Check ZSet for scores < current-time
	c_time := time.Now().Add(time.Duration(-3) * d.CleanupDur).UnixMilli()
	c_time_str := fmt.Sprintf("%v", c_time)
	dlog.Infof("consumer %s : checking for processors before %s", d.consumerId, c_time_str)
	zrb := &redis.ZRangeBy{
		Max: c_time_str,
	}
	ra, err := d.RedisClient.ZRangeByScoreWithScores(ctx, d.monitorZset, zrb).Result()
	if err != nil {
		dlog.Warnf("consumer %s : could not run zrangebyscore %v", d.consumerId, err)
		return
	}
	for _, rz := range ra {
		processor := rz.Member.(string)
		dlog.Warnf("consumer %s expired with last updated time %s", processor, rz.Score)
		if err := d.deleteProcessor(processor); err != nil {
			continue
		}
		// remove the processor
		d.RedisClient.ZRem(ctx, d.monitorZset, processor)
	}
	// Delete all processor with score < current-time
	d.RedisClient.ZRemRangeByScore(ctx, d.monitorZset, "0", c_time_str)
}
