package distr_ep

import (
	"errors"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

const (
	DEFAULT_LOCK_TTL       = time.Millisecond * 1000
	DEFAULT_LOCK_RETRY_DUR = time.Millisecond * 10
	DEFAULT_CLEANUP_DUR    = time.Second * 10
	DEFAULT_LIST_TTL       = time.Hour * 24
	DEFAULT_SCHEDULE_DUR   = time.Second * 1
)

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
	locker *redislock.Client
	// is initialized
	initialized bool
}

func (d *DistributedEventProcessor) Init() error {
	// Init all the resources
	if err := d.validate(); err != nil {
		log.Warnf("Validation failed %s", err)
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
		return errors.New("Namespace is required")
	}
	if d.Callback == nil {
		return errors.New("Callback is required")
	}
	if d.RedisClient == nil {
		return errors.New("Missign redis client")
	}
	if d.LockTTL == 0 {
		d.LockTTL = DEFAULT_LOCK_TTL
	}
	if d.CleanupDur == 0 {
		d.CleanupDur = DEFAULT_CLEANUP_DUR
	}
	d.locker = redislock.New(d.RedisClient)
	d.groupName = fmt.Sprintf("%s-cg", d.Namespace)
	d.monitorZset = fmt.Sprintf("%s:mon-zset", d.Namespace)
	d.monitorLock = fmt.Sprintf("%s:mon-zset:lk", d.Namespace)
	d.schedulerZset = fmt.Sprintf("%s:sch-zset", d.Namespace)
	d.schedulerLock = fmt.Sprintf("%s:sch-zset:lk", d.Namespace)
	d.schedulerHset = fmt.Sprintf("%s:sch-hset", d.Namespace)
	d.consumerId = xid.New().String()
	return nil
}

func (d *DistributedEventProcessor) AddEvent(key string, val interface{},
	delay time.Duration) error {
	if !d.initialized {
		return errors.New("Event Processor is not initialized")
	}
	if key == "" {
		return errors.New("key cant be empty")
	}
	if val == nil {
		return errors.New("Event val cant be null")
	}
	if delay > 0 {
		return d.scheduleEvent(key, val, delay)
	}
	// run the event now
	return d.runEvent(key, val)
}
