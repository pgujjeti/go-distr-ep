package distr_ep

import (
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
)

// Package global - can do better
var dlog = logrus.New()

type DistributedEventProcessor struct {
	// namespace
	Namespace string
	// redis connection
	RedisClient *redis.ClusterClient
	// TTL for lock
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
	// Event polling timeout
	EventPollTimeout time.Duration

	// Monitor ZSET
	monitorZset string
	monitorLock string
	// Scheduler
	schedulerZset string
	schedulerLock string
	schedulerHset string
	// Pending Keys
	pKeyList   string
	pkProcList string
	// Processing Set
	psKey string
	// Consumer id
	consumerId string
	// Locker
	locker *redsync.Redsync
	// is initialized
	initialized bool
}

func (d *DistributedEventProcessor) Init() error {
	dlog.SetLevel(d.LogLevel)
	// Init all the resources
	if err := d.validate(); err != nil {
		dlog.Warnf("Validation failed %s", err)
		return err
	}
	// TODO handle graceful termination of background go-routines
	// Start the clean-up goroutine
	go d.monitorKeys()
	// Start the scheduler gorouting
	go d.eventScheduler()
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
	pool := goredis.NewPool(d.RedisClient)
	d.consumerId = xid.New().String()
	d.locker = redsync.New(pool)
	d.pKeyList = fmt.Sprintf("dep:%s:pkeys", d.Namespace)
	d.pkProcList = fmt.Sprintf("dep:%s:pk-proc", d.Namespace)
	d.monitorZset = fmt.Sprintf("dep:%s:mon-zset", d.Namespace)
	d.monitorLock = fmt.Sprintf("dep:%s:mon-zset:lk", d.Namespace)
	d.schedulerZset = fmt.Sprintf("dep:%s:sch-zset", d.Namespace)
	d.schedulerLock = fmt.Sprintf("dep:%s:sch-zset:lk", d.Namespace)
	d.schedulerHset = fmt.Sprintf("dep:%s:sch-hset", d.Namespace)
	d.psKey = fmt.Sprintf("dep:%s:ps:%s", d.Namespace, d.consumerId)
	return nil
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
		return d.scheduleEvent(e, delay)
	}
	// run the event now
	return d.runEvent(e)
}
