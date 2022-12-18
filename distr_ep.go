package distr_ep

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/rs/xid"
	logrus "github.com/sirupsen/logrus"
)

const (
	LOCK_TTL       = time.Hour * 24
	LOCK_RETRY_DUR = time.Millisecond * 100
	CLEANUP_DUR    = time.Second * 10
	LIST_TTL       = time.Hour * 24
	SCHEDULE_DUR   = time.Second * 1
	EVT_POLL_TO    = time.Second * 3600 * 1
	// https://redis.io/docs/reference/cluster-spec/#hash-tags
	PK_HASH_PREFIX = "{dep:%s:pk-}"
)

// Package global - can do better
var dlog = logrus.New()

type DistributedEventProcessor struct {
	// namespace
	Namespace string
	// redis connection
	RedisClient *redis.ClusterClient
	// key lock duration
	LockTTL time.Duration
	// cleanup delay
	CleanupDur time.Duration
	// Event callback
	Callback EventCallback
	// LogLevel
	LogLevel logrus.Level
	// EventProcessingMode - set to true if retry is required
	// default is atmost once
	AtLeastOnce bool
	// Scheduling enabled
	Scheduling bool
	// Event polling timeout. Time to wait for new events for a key
	// This should be less than LockTTL
	EventPollTimeout time.Duration

	// Monitor ZSET
	keyMonitor *keyMonitor
	// Scheduler
	eventScheduler *eventScheduler
	// Pending Keys (shared list by processor)
	sharedPendingKeyList string
	// Active Keys Set
	activeKeyList string
	// Pending list -> to be processed list (specific to this processor)
	pkOffloadList string
	// Consumer id
	consumerId string
	// Locker
	locker *redsync.Redsync
	// is initialized
	initialized bool
	// key consumer cancel fn
	keyCancelFn context.CancelFunc
}

func (d *DistributedEventProcessor) Init() error {
	dlog.SetLevel(d.LogLevel)
	// Init all the resources
	if err := d.validate(); err != nil {
		dlog.Warnf("Validation failed %s", err)
		return err
	}
	// Start the clean-up goroutine
	d.keyMonitor.start()
	// Start the scheduler gorouting
	d.eventScheduler.start()
	// Start the Pending Key processor
	go d.pendingKeysConsumer()
	d.initialized = true
	return nil
}

func (d *DistributedEventProcessor) validate() error {
	// Init all the resources
	if len(d.Namespace) == 0 {
		return errors.New("namespace is required")
	}
	if d.Callback == nil {
		return errors.New("callback is required")
	}
	if d.RedisClient == nil {
		return errors.New("redis client is required")
	}
	if d.LockTTL == 0 {
		d.LockTTL = LOCK_TTL
	}
	if d.CleanupDur == 0 {
		d.CleanupDur = CLEANUP_DUR
	}
	if d.EventPollTimeout == 0 {
		d.EventPollTimeout = EVT_POLL_TO
	}
	if d.EventPollTimeout > d.LockTTL {
		return errors.New("event poll timeout greater than lock timeout")
	}
	pool := goredis.NewPool(d.RedisClient)
	d.consumerId = xid.New().String()
	d.locker = redsync.New(pool)
	d.sharedPendingKeyList = fmt.Sprintf(PK_HASH_PREFIX+"pending", d.Namespace)
	d.pkOffloadList = d.procOffloadListKey(d.consumerId)
	d.activeKeyList = d.processorSetKey(d.consumerId)
	d.keyMonitor = &keyMonitor{
		dur:      d.CleanupDur,
		d:        d,
		zsetKey:  fmt.Sprintf("dep:%s:mon-zset", d.Namespace),
		lockName: fmt.Sprintf("dep:%s:mon-zset:lk", d.Namespace),
	}
	d.eventScheduler = &eventScheduler{
		enabled:  d.Scheduling,
		dur:      SCHEDULE_DUR,
		d:        d,
		zsetKey:  fmt.Sprintf("dep:%s:sch-zset", d.Namespace),
		lockName: fmt.Sprintf("dep:%s:sch-zset:lk", d.Namespace),
		hsetKey:  fmt.Sprintf("dep:%s:sch-hset", d.Namespace),
	}
	return nil
}

func (d *DistributedEventProcessor) Shutdown() {
	dlog.Warnf("%s : shutting down processor...", d.consumerId)
	// stop pendingKeysConsumer
	// it stop processing events for active keys
	if d.keyCancelFn != nil {
		d.keyCancelFn()
	}
	// stop event scheduler
	d.eventScheduler.stop()
	// stop monitor process
	d.keyMonitor.stop()
}

func (d *DistributedEventProcessor) AddEvent(e *DistrEvent) error {
	return d.ScheduleEvent(e, 0)
}

func (d *DistributedEventProcessor) ScheduleEvent(e *DistrEvent,
	delay time.Duration) error {
	if !d.initialized {
		return errors.New("not-initialized")
	}
	if e.Key == "" {
		return errors.New("key cant be empty")
	}
	if e.Val == nil {
		return errors.New("val is nil")
	}
	if delay > 0 {
		return d.eventScheduler.scheduleEvent(e, delay)
	}
	// run the event now
	return d.runEvent(e)
}
