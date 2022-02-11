package distr_ep

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
)

type protectedJobRunner interface {
	runJob(ch chan bool)
}

// runs a job protected by the named lock
// the job is executed synchronously by running a ticker to renew the lock
func runProtectedJob(locker *redsync.Redsync, lockName string,
	dur time.Duration, p protectedJobRunner) {
	defer timeExecution(time.Now(), fmt.Sprintf("%s-run", lockName))
	ctx := context.Background()
	// Try to acquire lock
	lock := locker.NewMutex(lockName, redsync.WithExpiry(dur))
	// Lock not acquired? return
	if err := lock.LockContext(ctx); err != nil {
		dlog.Debugf("could not obtain %s lock: %v", lockName, err)
		return
	}
	defer lock.UnlockContext(ctx)
	runProtectedJobWithLock(lock, dur, p)
}

// run job with the passed lock (instead of lockname)
func runProtectedJobWithLock(lock *redsync.Mutex,
	dur time.Duration, p protectedJobRunner) {
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
			dlog.Debugf("Renewing lock %s", lock.Name())
			lock.ExtendContext(ctx)
		case <-ch_done:
			// Job complete
			ticker.Stop()
			dlog.Debugf("Protected job COMPLETED %s", lock.Name())
			return
		}
	}
}
