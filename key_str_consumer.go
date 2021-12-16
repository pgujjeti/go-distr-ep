package distr_ep

import (
	"context"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func (d *DistributedEventProcessor) keyStreamConsumer() {
	// consume events from {NS}:key-stream stream
	for {
		ctx := context.Background()
		rga := &redis.XReadGroupArgs{
			Group:    d.groupName,
			Consumer: d.consumerId,
			Streams:  []string{d.keyStream, ">"},
			Count:    2,
			Block:    0,
			NoAck:    false,
		}
		xs, err := d.RedisClient.XReadGroup(ctx, rga).Result()
		if err != nil || len(xs) == 0 || len(xs[0].Messages) == 0 {
			// TODO err handling
			log.Debugf("No messages to process in key stream")
			continue
		}
		for _, msg := range xs[0].Messages {
			// handle message
			d.processKeyMessage(ctx, msg)
		}
	}
}

func (d *DistributedEventProcessor) processKeyMessage(ctx context.Context,
	msg redis.XMessage) {
	key, val := msg.Values["key"].(string), msg.Values["val"]
	// add the event into key-specific {NS}:events:{key} stream
	a := &redis.XAddArgs{
		Stream: d.streamNameForKey(key),
		Values: map[string]interface{}{"val": val},
	}
	d.RedisClient.XAdd(ctx, a)
	// add to the monitor ZSET {NS}:k-monitor
	d.refreshKeyMonitorExpiry(ctx, key)
	// Kick off the event handler
	go d.keyEventHandler(key)
	d.RedisClient.XAck(ctx, d.keyStream, d.groupName, msg.ID)
}
