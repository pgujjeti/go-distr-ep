package distr_ep

import (
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/rs/xid"
	logrus "github.com/sirupsen/logrus"
)

const (
	LOCK_TTL       = time.Hour * 2
	LOCK_RETRY_DUR = time.Millisecond * 100
	CLEANUP_DUR    = time.Second * 10
	LIST_TTL       = time.Hour * 24
	SCHEDULE_DUR   = time.Second * 1
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
	// keyProcessor
	keyProcessor *keyProcessor
	// Consumer Id
	consumerId string
	// Locker
	locker *redsync.Redsync
	// is initialized
	initialized bool
	// keys -> node hash set
	keyToNodeSetName string
}

func (d *DistributedEventProcessor) Init() error {
	dlog.SetLevel(d.LogLevel)
	dlog.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02T15:04:05.999999Z07:00",
	})
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
	d.keyProcessor.start()
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
		d.EventPollTimeout = d.LockTTL / 2
	}
	if d.EventPollTimeout > d.LockTTL/2 {
		return errors.New("event poll timeout should be <= (lock TTL)/2")
	}
	pool := goredis.NewPool(d.RedisClient)
	d.consumerId = xid.New().String()
	d.locker = redsync.New(pool)
	d.keyMonitor = &keyMonitor{
		dur:      d.CleanupDur,
		d:        d,
		zsetKey:  d.monZsetKeyName(),
		lockName: d.monZsetLockName(),
	}
	d.eventScheduler = &eventScheduler{
		enabled:  d.Scheduling,
		dur:      SCHEDULE_DUR,
		d:        d,
		zsetKey:  d.schZsetKeyName(),
		lockName: d.schZsetLockName(),
		hsetKey:  d.schHashKeyName(),
	}
	d.keyProcessor = &keyProcessor{
		d:             d,
		activeKeyList: d.processorSetKey(d.consumerId),
		keyEventsList: d.processorEventsListName(d.consumerId),
	}
	d.keyToNodeSetName = d.keyNodeHashName()
	return nil
}

func (d *DistributedEventProcessor) Shutdown() {
	dlog.Warnf("%s : shutting down processor...", d.consumerId)
	// stop pendingKeysConsumer
	d.keyProcessor.stop()
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
