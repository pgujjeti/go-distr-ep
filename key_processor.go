package distr_ep

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	KP_DEFAULT_EVENT_TO = 300 * time.Second
)

type keyProcessor struct {
	// Pending Keys (shared list by processor)
	// sharedPendingKeyList string
	// Active Keys Set
	activeKeyList string
	// Notifications about events for keys handled by this node
	keyEventsList    string
	d                *DistributedEventProcessor
	keyCancelFn      context.CancelFunc
	keysMap          map[string]*keyWrapper
	eventWaitTimeout time.Duration
}

type keyWrapper struct {
	notif_ch chan bool
	closed   bool
}

func (k *keyProcessor) getWrapper(key string) *keyWrapper {
	return k.keysMap[key]
}

func (k *keyProcessor) start() {
	k.eventWaitTimeout = KP_DEFAULT_EVENT_TO
	k.keysMap = map[string]*keyWrapper{}
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

// consume events from this node's notif queue
func (k *keyProcessor) pendingKeysConsumer(ctx context.Context) {
	d := k.d
	// run the consumer loop
	for {
		// move from main list to this processor's offload list and delete the item at the end
		result, err := d.RedisClient.BLPop(ctx, k.eventWaitTimeout, k.keyEventsList).Result()
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
		key := result[1]
		dlog.Debugf("%s : Processing key: %v", d.consumerId, key)
		go d.eventNotifForKey(ctx, key)
	}
}

func (k *keyProcessor) addKeyToProcessor(ctx context.Context, key string) error {
	k.keysMap[key] = &keyWrapper{
		notif_ch: make(chan bool),
		closed:   false,
	}
	d := k.d
	// ADD processing node info for this key
	d.addNodeProcessingKey(ctx, key)
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
	// DELETE node affinity data; watch for race condition
	d.removeNodeProcessingKey(ctx, key)
	delete(k.keysMap, key)
	return err
}

func (k *keyProcessor) deleteProcessor(consumerId string) error {
	dlog.Warnf("Deleting consumer %s...", consumerId)
	d := k.d
	// * Get the list of active KEY_IDs
	// * For each KEY_ID
	// 	* Inject an event into Pending XPK stream
	// 	* Delete the KEY_ID lock
	if err := k.reprocessKeysFromList(consumerId, d.processorSetKey(consumerId)); err != nil {
		return err
	}
	// delete the processor events list
	evt_ln := d.processorEventsListName(consumerId)
	dlog.Infof("Deleting events list %s for consumer %s", evt_ln, consumerId)
	d.RedisClient.Del(context.Background(), evt_ln)
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
	// run key handler
	d.keyEventHandler(ctx, key)
}
