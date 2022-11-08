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
	DEFAULT_LOCK_TTL       = time.Millisecond * 1000
	DEFAULT_LOCK_RETRY_DUR = time.Millisecond * 100
	DEFAULT_CLEANUP_DUR    = time.Second * 10
	DEFAULT_LIST_TTL       = time.Hour * 24
	DEFAULT_SCHEDULE_DUR   = time.Second * 1
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

	// Group name
	groupName string
	// Monitor ZSET
	monitorZset string
	monitorLock string
	// Scheduler
	schedulerZset string
	schedulerLock string
	schedulerHset string
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
		d.LockTTL = DEFAULT_LOCK_TTL
	}
	if d.CleanupDur == 0 {
		d.CleanupDur = DEFAULT_CLEANUP_DUR
	}
	pool := goredis.NewPool(d.RedisClient)
	d.locker = redsync.New(pool)
	d.groupName = fmt.Sprintf("%s-cg", d.Namespace)
	d.monitorZset = fmt.Sprintf("%s:mon-zset", d.Namespace)
	d.monitorLock = fmt.Sprintf("%s:mon-zset:lk", d.Namespace)
	d.schedulerZset = fmt.Sprintf("%s:sch-zset", d.Namespace)
	d.schedulerLock = fmt.Sprintf("%s:sch-zset:lk", d.Namespace)
	d.schedulerHset = fmt.Sprintf("%s:sch-hset", d.Namespace)
	d.consumerId = xid.New().String()
	return nil
}

func (d *DistributedEventProcessor) AddEvent(key string, val interface{}) error {
	return d.ScheduleEvent(key, val, 0)
}

func (d *DistributedEventProcessor) ScheduleEvent(key string, val interface{},
	delay time.Duration) error {
	if !d.initialized {
		return errors.New("not-initialized")
	}
	if key == "" {
		return errors.New("key cant be empty")
	}
	if val == nil {
		return errors.New("val is nil")
	}
	if delay > 0 {
		return d.scheduleEvent(key, val, delay)
	}
	// run the event now
	return d.runEvent(key, val)
}
