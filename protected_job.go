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

// runs a job protected by the named lock
// the job is executed synchronously by running a ticker to renew the lock
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
	runProtectedJobWithLock(lock, dur, p)
}

// run job with the passed lock (instead of lockname)
func runProtectedJobWithLock(lock *redislock.Lock,
	dur time.Duration, p ProtectedJobRunner) {
	ctx := context.Background()
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
			log.Debugf("Renewing lock %s", lock.Key())
			lock.Refresh(ctx, dur, nil)
		case <-ch_done:
			// Job complete
			ticker.Stop()
			log.Debugf("Protected job COMPLETED %s", lock.Key())
			return
		}
	}
}
