package distr_ep

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

type keyNodeInfo struct {
	NodeId string `json:"node,omitempty"`
}

var (
	errKeyTimedOut = errors.New("key timed out")
	errChClosed    = errors.New("channel closed")
)

func (d *DistributedEventProcessor) findNodeProcessingKey(
	ctx context.Context, key string) (*keyNodeInfo, error) {
	// Use a redis hash
	res, err := d.RedisClient.HGet(ctx, d.keyToNodeSetName, key).Result()
	if err != nil {
		return nil, err
	}
	kn := &keyNodeInfo{}
	if err = json.Unmarshal([]byte(res), kn); err != nil {
		return nil, err
	}
	return kn, nil
}

func (d *DistributedEventProcessor) notifyEventForNode(ctx context.Context, node *keyNodeInfo, key string) error {
	evt_ln := d.processorEventsListName(node.NodeId)
	if d.nodeMatches(node.NodeId) {
		// if this is the node, skip the queue & notify directly
		go d.eventNotifForKey(ctx, key)
		return nil
	}
	dlog.Debugf("%s : sending a notification to node %s", key, evt_ln)
	return d.RedisClient.RPush(ctx, evt_ln, key).Err()
}

func (d *DistributedEventProcessor) nodeMatches(nodeId string) bool {
	return strings.EqualFold(d.consumerId, nodeId)
}

func (d *DistributedEventProcessor) addNodeProcessingKey(ctx context.Context, key string) error {
	kn := &keyNodeInfo{NodeId: d.consumerId}
	val, err := json.Marshal(kn)
	if err != nil {
		return err
	}
	dlog.Debugf("%s : adding node mapping %s", key, val)
	return d.RedisClient.HSet(ctx, d.keyToNodeSetName, key, string(val)).Err()
}

func (d *DistributedEventProcessor) removeNodeProcessingKey(ctx context.Context, key string) error {
	dlog.Debugf("%s : removing node mapping", key)
	return d.RedisClient.HDel(ctx, d.keyToNodeSetName, key).Err()
}

func (d *DistributedEventProcessor) eventNotifForKey(ctx context.Context, key string) {
	dlog.Debugf("%s : received event notification", key)
	rec_to := 20 * time.Millisecond
	kw := d.keyProcessor.getWrapper(key)
	submit_job := true
	if kw != nil {
		select {
		case _, ok := <-kw.notif_ch:
			if ok {
				// processor notified
				dlog.Infof("%s : processor notified", key)
				submit_job = false
			} else {
				dlog.Warnf("%s : processor channel stopped. restart job", key)
			}
		case <-time.After(rec_to):
			// timed out
			if kw.closed {
				dlog.Warnf("%s : processor closed. restart job", key)
			} else {
				submit_job = false
			}
		}
	}
	if submit_job {
		d.keyEventHandler(ctx, key)
	}
}

func (d *DistributedEventProcessor) waitForNewEvent(ctx context.Context, key, listname string) error {
	if d.RedisClient.LLen(ctx, listname).Val() > 0 {
		return nil
	}
	kw := d.keyProcessor.getWrapper(key)
	if kw.closed {
		return errChClosed
	}
	for {
		select {
		case kw.notif_ch <- true:
			dlog.Infof("%s : new event received", key)
			// new event notified; check length of key events list
			if d.RedisClient.LLen(ctx, listname).Val() > 0 {
				return nil
			}
			dlog.Debugf("%s : empty list; repeat the wait", key)
		case <-time.After(d.EventPollTimeout):
			// timed out
			kw.closed = true
			close(kw.notif_ch)
			dlog.Warnf("%s : timed out waiting for events", key)
			return errKeyTimedOut
		}
	}
}
