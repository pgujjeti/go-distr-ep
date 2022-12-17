package distr_ep

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func (d *DistributedEventProcessor) pendingKey(key string) error {
	ln := d.sharedPendingKeyList
	ctx := context.Background()
	// add the key to pending keys
	r, err := d.RedisClient.RPush(ctx, ln, key).Result()
	dlog.Infof("%s : add key to pending keys %s result: %v, %v", key, ln, r, err)
	if err != nil {
		dlog.Warnf("%s : could not add key to pending keys: %v", key, err)
		return err
	}
	return nil
}

// consume and run pending keys
// multiple processors shall de-queue from the shared pending key list using Blocking Move
func (d *DistributedEventProcessor) pendingKeysConsumer() {
	// run the consumer loop
	ctx, cancel := context.WithCancel(context.Background())
	d.keyCancelFn = cancel
	for {
		// move from main list to this processor's offload list and delete the item at the end
		key, err := d.RedisClient.BLMove(ctx, d.sharedPendingKeyList, d.pkOffloadList, REDIS_POS_LEFT,
			REDIS_POS_RIGHT, 0).Result()
		// keys, err := d.RedisClient.BLPop(ctx, 0, d.pKeyList).Result()
		if ctx.Err() != nil {
			dlog.Warnf("%s : context cancelled: %v", d.consumerId, err)
			break
		}
		if err == redis.Nil {
			dlog.Debugf("%s : no pending keys; retrying", d.consumerId)
			continue
		}
		if err != nil {
			dlog.Warnf("%s : fetching pending keys failed.. retrying: %v", d.consumerId, err)
			continue
		}
		dlog.Infof("%s : Processing key: %v", d.consumerId, key)
		d.keyEventHandler(ctx, key)
		// delete the key from processor's offload list
		r, err := d.RedisClient.LTrim(ctx, d.pkOffloadList, 1, 0).Result()
		dlog.Debugf("%s : deleted item from offload list %s %s : %v", d.consumerId,
			d.pkOffloadList, r, err)
	}
}

func (d *DistributedEventProcessor) addKeyToProcessor(ctx context.Context, key string) error {
	ps_key := d.activeKeyList
	dlog.Infof("%s : adding key %s to list of keys at %s", d.consumerId, key, ps_key)
	r, err := d.RedisClient.RPush(ctx, ps_key, key).Result()
	dlog.Debugf("%s : added key %s with result %v: %v", d.consumerId, key, r, err)
	return err
}

func (d *DistributedEventProcessor) removeKeyForProcessor(ctx context.Context, key string) error {
	ps_key := d.activeKeyList
	dlog.Debugf("%s : removing key %s from processor list %s", d.consumerId, key, ps_key)
	r, err := d.RedisClient.LRem(ctx, ps_key, 1, key).Result()
	// r, err := d.RedisClient.SRem(ctx, ps_key, key).Result()
	dlog.Infof("%s : removed key %s with result %v: %v", d.consumerId, key, r, err)
	return err
}

func (d *DistributedEventProcessor) deleteProcessor(consumerId string) error {
	// * Get the list of active KEY_IDs
	// * For each KEY_ID
	// 	* Inject an event into Pending XPK stream
	// 	* Delete the KEY_ID lock
	// * Repeat this for keys offloaded from pending keys that were in flight
	if err := d.reprocessKeysFromList(consumerId, d.processorSetKey(consumerId)); err != nil {
		return err
	}
	// check processor's offload list
	if err := d.reprocessKeysFromList(consumerId, d.procOffloadListKey(consumerId)); err != nil {
		return err
	}
	return nil
}

func (d *DistributedEventProcessor) reprocessKeysFromList(consumerId, ps_key string) error {
	ctx := context.Background()
	for {
		// access the first key
		key, err := d.RedisClient.LIndex(ctx, ps_key, 0).Result()
		if err == redis.Nil {
			dlog.Infof("No keys to hand off for processor %s", consumerId)
			return nil
		}
		if err != nil {
			dlog.Errorf("%s : Error while fetching keys %v", consumerId, err)
			return err
		}
		dlog.Infof("processor %s is handling key: %v", consumerId, key)
		d.reprocessKey(key)
		// remove the key (the first element)
		err = d.RedisClient.LPop(ctx, ps_key).Err()
		dlog.Infof("Deleted key %s from processor %s: %v", key, consumerId, err)
	}
}

func (d *DistributedEventProcessor) reprocessKey(key string) {
	d.pendingKey(key)
	// delete the lock
	lock := d.locker.NewMutex(d.processLockForKey(key))
	lock.Unlock()
}

func (d *DistributedEventProcessor) processorSetKey(consumerId string) string {
	return fmt.Sprintf("dep:%s:pk-active:%s", d.Namespace, consumerId)
}

func (d *DistributedEventProcessor) procOffloadListKey(consumerId string) string {
	return fmt.Sprintf(PK_HASH_PREFIX+"ol:%s", d.Namespace, consumerId)
}
