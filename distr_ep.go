package distr_ep

import (
	"context"
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
)

type DistributedEventProcessor struct {
	// namespace
	Namespace string
	// redis connection
	RedisClient *redis.Client
	// TTL for lock
	LockTTL time.Duration
	// cleanup delay
	CleanupDur time.Duration
	// Event callback
	Callback EventCallback

	// Group name
	groupName string
	// Monitor ZSET
	monitorZset     string
	monitorLockName string
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
	// Start the clean-up goroutine
	go d.monitorKeys()
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
	d.monitorZset = fmt.Sprintf("%s:mon-set", d.Namespace)
	d.monitorLockName = fmt.Sprintf("%s:mon-set:lk", d.Namespace)
	d.consumerId = xid.New().String()
	return nil
}

func (d *DistributedEventProcessor) AddEvent(key string, val interface{}) error {
	if !d.initialized {
		return errors.New("Event Processor is not initialized")
	}
	// add the event into key-specific {NS}:events:{key} entity
	ctx := context.Background()
	ln := d.listNameForKey(key)
	// Add new events to the end of the list
	r, err := d.RedisClient.RPush(ctx, ln, val).Result()
	log.Debugf("%s : add event for processing result: %v, %v", key, r, err)
	if err != nil {
		log.Warnf("%s : could not add event for processing: %v", key, err)
		return err
	}
	// renew List expiry
	d.RedisClient.Expire(ctx, ln, DEFAULT_LIST_TTL)
	// add to the monitor ZSET {NS}:k-monitor
	d.refreshKeyMonitorExpiry(ctx, key)
	// Kick off the event handler
	go d.keyEventHandler(key)
	return nil
}
