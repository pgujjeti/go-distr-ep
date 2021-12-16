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

type DistributedEventProcessor struct {
	// namespace
	Namespace string
	// redis connection
	RedisClient *redis.Client
	// TTL for lock
	LockTTL time.Duration
	// cleanup delay
	CleanupDur int
	Callback   EventCallback

	// Key stream
	keyStream string
	// Group name
	groupName string
	// Monitor ZSET
	monitorZset     string
	monitorLockName string
	// Consumer id
	consumerId string
	// Locker
	locker *redislock.Client
}

func (d *DistributedEventProcessor) Init() error {
	// Init all the resources
	if len(d.Namespace) == 0 {
		return errors.New("Namespace is required")
	}
	d.locker = redislock.New(d.RedisClient)
	d.keyStream = fmt.Sprintf("%s:k-str", d.Namespace)
	d.groupName = fmt.Sprintf("%s-cg", d.Namespace)
	d.monitorZset = fmt.Sprintf("%s:mon-set", d.Namespace)
	d.monitorLockName = fmt.Sprintf("%s:mon-set:lk", d.Namespace)
	d.consumerId = xid.New().String()

	// Context
	ctx := context.Background()
	// Create consumer group, key stream
	gia, err := d.RedisClient.XInfoGroups(ctx, d.keyStream).Result()
	if err != nil {
		return err
	}
	groupExists := false
	for _, gi := range gia {
		if gi.Name == d.groupName {
			// Group exists
			groupExists = true
			break
		}
	}
	if !groupExists {
		d.RedisClient.XGroupCreateMkStream(ctx, d.keyStream, d.groupName, "0")
	}

	// Start the key-stream consumer
	go d.keyStreamConsumer()
	// Start the clean-up goroutine
	go d.monitorKeys()
	return nil
}

func (d *DistributedEventProcessor) AddEvent(key string, val interface{}) error {
	// Add the element to key-stream
	a := &redis.XAddArgs{
		Stream: d.keyStream,
		Values: map[string]interface{}{"key": key, "val": val},
	}
	r, err := d.RedisClient.XAdd(context.Background(), a).Result()
	log.Debugf("Added event to %s: %v, %s", d.keyStream, r, err)
	return err
}