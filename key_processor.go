package distr_ep

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type keyProcessor struct {
	// Pending Keys (shared list by processor)
	sharedPendingKeyList string
	// Active Keys Set
	activeKeyList string
	// Pending list -> to be processed list (specific to this processor)
	pkOffloadList string
	d             *DistributedEventProcessor
	keyCancelFn   context.CancelFunc
}

func (k *keyProcessor) start() {
	ctx, cancel := context.WithCancel(context.Background())
	k.keyCancelFn = cancel
	go k.pendingKeysConsumer(ctx)
}

// stop consuming new keys
// stop currently active key event processing
func (k *keyProcessor) stop() {
	dlog.Infof("%s - stopping key processor", k.d.consumerId)
	k.keyCancelFn()
}

func (k *keyProcessor) pendingKey(key string) error {
	d := k.d
	ln := k.sharedPendingKeyList
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
func (k *keyProcessor) pendingKeysConsumer(ctx context.Context) {
	d := k.d
	// run the consumer loop
	for {
		// move from main list to this processor's offload list and delete the item at the end
		key, err := d.RedisClient.BLMove(ctx, k.sharedPendingKeyList, k.pkOffloadList, REDIS_POS_LEFT,
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
		r, err := d.RedisClient.LTrim(ctx, k.pkOffloadList, 1, 0).Result()
		dlog.Debugf("%s : deleted item from offload list %s %s : %v", d.consumerId,
			k.pkOffloadList, r, err)
	}
}

func (k *keyProcessor) addKeyToProcessor(ctx context.Context, key string) error {
	d := k.d
	ps_key := k.activeKeyList
	dlog.Infof("%s : adding key %s to list of keys at %s", d.consumerId, key, ps_key)
	r, err := d.RedisClient.RPush(ctx, ps_key, key).Result()
	dlog.Debugf("%s : added key %s with result %v: %v", d.consumerId, key, r, err)
	return err
}

func (k *keyProcessor) removeKeyForProcessor(ctx context.Context, key string) error {
	d := k.d
	ps_key := k.activeKeyList
	dlog.Debugf("%s : removing key %s from processor list %s", d.consumerId, key, ps_key)
	r, err := d.RedisClient.LRem(ctx, ps_key, 1, key).Result()
	// r, err := d.RedisClient.SRem(ctx, ps_key, key).Result()
	dlog.Infof("%s : removed key %s with result %v: %v", d.consumerId, key, r, err)
	return err
}

func (k *keyProcessor) deleteProcessor(consumerId string) error {
	d := k.d
	// * Get the list of active KEY_IDs
	// * For each KEY_ID
	// 	* Inject an event into Pending XPK stream
	// 	* Delete the KEY_ID lock
	// * Repeat this for keys offloaded from pending keys that were in flight
	if err := k.reprocessKeysFromList(consumerId, d.processorSetKey(consumerId)); err != nil {
		return err
	}
	// check processor's offload list
	if err := k.reprocessKeysFromList(consumerId, d.procOffloadListKey(consumerId)); err != nil {
		return err
	}
	return nil
}

func (k *keyProcessor) reprocessKeysFromList(consumerId, ps_key string) error {
	d := k.d
	ctx := context.Background()
	for {
		// access the first key
		key, err := d.RedisClient.LIndex(ctx, ps_key, 0).Result()
		if err == redis.Nil {
			dlog.Infof("No keys to hand off for processor %s from list %s", consumerId, ps_key)
			return nil
		}
		if err != nil {
			dlog.Errorf("%s : Error while fetching keys from %s: %v", consumerId, ps_key, err)
			return err
		}
		dlog.Infof("processor %s is handling key: %s from list %s", consumerId, key, ps_key)
		k.reprocessKey(key)
		// remove the key (the first element)
		err = d.RedisClient.LPop(ctx, ps_key).Err()
		dlog.Infof("Deleted key %s from processor %s: %v", key, consumerId, err)
	}
}

func (k *keyProcessor) reprocessKey(key string) {
	d := k.d
	dlog.Infof("%s : reprocessing the key %s", d.consumerId, key)
	// delete the lock
	pl_key := d.processLockForKey(key)
	ctx := context.Background()
	r, err := d.RedisClient.Del(ctx, pl_key).Result()
	dlog.Debugf("Deleted lock %s for key %s: %s; %v", pl_key, key, r, err)
	k.pendingKey(key)
}

func (d *DistributedEventProcessor) processorSetKey(consumerId string) string {
	return fmt.Sprintf("dep:%s:pk-active:%s", d.Namespace, consumerId)
}

func (d *DistributedEventProcessor) procOffloadListKey(consumerId string) string {
	return fmt.Sprintf(PK_HASH_PREFIX+"ol:%s", d.Namespace, consumerId)
}
