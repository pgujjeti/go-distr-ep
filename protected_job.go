package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	log "github.com/sirupsen/logrus"
)

type ProtectedJobRunner interface {
	runJob(ch chan bool)
}

// reusable code (scheduler, monitor, event-processor)
func runProtectedJob(locker *redislock.Client, lockName string,
	dur time.Duration, p ProtectedJobRunner) {
	defer timeExecution(time.Now(), fmt.Sprintf("%s-run", lockName))
	ctx := context.Background()
	// Try to acquire lock
	lock, err := locker.Obtain(ctx, lockName, dur, nil)
	// Lock not acquired? return
	if err == redislock.ErrNotObtained {
		log.Debugf("could not obtain %s lock", lockName)
		return
	}
	defer lock.Release(ctx)
	// Refresh the lock while the job runs with a ticker
	// running at 1/2 the lock duration
	ticker := time.NewTicker(dur / 2)
	defer ticker.Stop()
	// channel to indicate when the job routine is completed
	ch_done := make(chan bool)
	go p.runJob(ch_done)
	for {
		select {
		case <-ticker.C:
			// job is still running - renew lock
			log.Debugf("Renewing lock %s", lockName)
			lock.Refresh(ctx, dur, nil)
		case <-ch_done:
			// Job complete
			ticker.Stop()
			log.Debugf("Job COMPLETED %s", lockName)
			return
		}
	}
}
