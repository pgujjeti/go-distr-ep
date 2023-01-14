# go-distr-ep
This project provides a distributed event processor framework for golang. This 
framework would be useful for cases where you need to process events 
sequentially, for a given key.

go-distr-ep uses redis for persisting events (fault-tolerance) and for distributing 
them among all the participating processors.

## Features
- Events are processed in order, for a given key
- Events for a given key are processed sequentially (i.e. not concurrently)
- Events can be scheduled to be executed after a delay
- Fault-tolerance (Events are persisted to an external cache, redis)

## Dependencies
- [go-redis/v8](https://github.com/go-redis/redis)
- [redsync](https://github.com/go-redsync/redsync) 

## Example
```go
type TestCallbackImpl struct {
	callbackName string
}

func (t *TestCallbackImpl) StartProcessing(key string) {
	// start key processing - initialize things like cache, etc that you need
}

func (t *TestCallbackImpl) ProcessEvent(key string, val interface{}) bool {
	// process event
	return false
	// return true when processing is completed for this key
	// return true
}

func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	callbackImpl := &TestCallbackImpl{callbackName: name}
	dep := &distr_ep.DistributedEventProcessor{
		RedisClient: client,
		Namespace:   "test1",
		// LockTTL:     time.Second * 1000,
		CleanupDur:  time.Second * 1,
		Callback:    callbackImpl,
		LogLevel:    log.InfoLevel,
		// AtLeastOnce: true,
		Scheduling:  true,
	}
	if err := dep.Init(); err != nil {
		log.Errorf("error initializing.. %v", err)
		return
	}

	// Add an event to processing
	// Start: true to indicate this event is a trigger event for processing 
	evt := &distr_ep.DistrEvent{Key: "key1", Val: val, Start: true}
	dep.AddEvent(evt))
	// Schedule an event for processing after 5 seconds
	dep.ScheduleEvent(evt, time.Second * 5)
}
```
## Local Test
You can run a local redis cluster using [grokzen/redis-cluster](https://github.com/Grokzen/docker-redis-cluster)
```
docker run -d --name redis-cluster -e "IP=0.0.0.0" -p 7000-7005:7000-7005 grokzen/redis-cluster:latest
```
Note: On MacOS, port 7000 is alredy bound by [AirPlay receiver](https://developer.apple.com/forums/thread/682332)

# Design
The Distributed Event Processor (DEP) framework's goals are:
- Distributed: Events can be submitted by multiple clients
- Sequential: Events should be processed in-order, for a _given_ key
- Resilient: Event processing should be resilient (fail-over)
- Sticky: Process events for a given key by a given processor, within the prescribed TTL

## Components
DEP is designed to be a simple framework with minimal overhead. It uses Redis to store events and to manage distributed locks.

We use `go-redis` and `redsync` libraries for Redis client operations and distributed lock, respectively

## Event Submission
When a new event is submitted for a given key
- If this is a `start` event, the Key is added to list of `PendingKeys`
- The event pushed to the end of a redis LIST specific to this Key
- The TTL of the LIST key is renewed to the configured value

## PendingKey Consumer
PendingKeys Consumer's job is to monitor for any pending keys that need to be processed. It runs on all the participating nodes/processors.
- Move a Key from `PendingKeys` to a consumer specific LIST
- Kick off the Key Processor for this Key
- Remove the Key from the consumer specific LIST

## Key Processor
Key processor attempts to acquire the lock for the specified key and if successful, processes the pending events
- Attempt to acquire redis lock for this Key
- If Key lock is NOT acquired: stops processing
- If Key lock is acquired, adds this Key to the processor's list of active Keys
- Start processing events from the Key's event LIST. It uses a blocking wait of `EventPollTimeout` (default 1 hour)
- If there are no events after `EventPollTimeout`, stop the processor
- For each event in the list (atmost-once sematics):
  - Pop the event
  - Invoke the `Callback.ProcessEvent(key, val)` as a go-routine
  - Rewew the redis Key lock, while the event is being processed
  - If `ProcessEvent()` returns `true` to indicate that this key is now completely processed, stop the processor
- When processing stops for the Key:
  - Release the Key lock
  - Remove the Key from the processor's list of active keys

## Key Lock
The redis key lock is the central piece to achieve sequential processing. It allows us to prevent concurrent execution for the same key. Only one event processor at a time can acquire the lock and thus process events for that key.

We use the open source library [redsync](https://github.com/go-redsync/redsync) for distributed locks

## Client Monitor
Client Monitor is a background process that monitors any Clients that might have terminated midway.
- Each Client does a periodic checkin, updating its TTL in a redis ZSET
- If a Client is detected as inactive, all the Keys that it was processing shall be re-submitted to the Pending Keys list (which is de-queued by Pending Key Consumers)

## Scheduled Events (Optional feature)
DEP also supports scheduled events. The Event Scheduler runs a periodic check for scheduled events. It also uses redis ZSET (like the Key Monitor) to monitor for events that are expriring.

## Redis Keys
Following is a table of Redis keys used

| Item   | Redis Key        | Remarks |
|--------|------------------|---------|
| Key Event List | `dep:{DEP_NS}:key-el:{KEY}` | Redis LIST to store events by Key |
| Key Lock | `dep:{DEP_NS}:key-lk:{KEY}` | Redis lock name buy Key  |
| Pending Keys | `{dep:{DEP_NS}:pk}-pending` | Redis LIST for pending Keys that need to be processed |
| Client: Pending Keys Offload | `{dep:{DEP_NS}:pk}-ol:{CLIENT_ID}` | Redis LIST for offloaded pending Keys (Client specific) |
| Client: Active Keys | `dep:{DEP_NS}:pk-active:{CLIENT_ID}` | Redis List for Client's Active Keys |
| Client Monitor: Active Clients ZSet | `dep:{DEP_NS}:mon-zset` | Client Monitor: Redis ZSet. Clients update their TTL periodically |
| Client Monitor: Lock | `dep:{DEP_NS}:mon-zset:lk` | Client Monitor Lock. Required to run the monitoring job |
| Scheduled Events ZSet | `dep:{DEP_NS}:sch-zset` | Scheduler Events ZSet: Expiry is determined by Epoch |
| Scheduled Event Payload HSet | `dep:{DEP_NS}:sch-hset` | Scheduler Events Payload |
| Scheduler Lock | `dep:{DEP_NS}:sch-zset:lk` | Schduler Job Lock |
