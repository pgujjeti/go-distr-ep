# go-distr-ep
This project provides a distributed event processor framework for golang. This 
framework would be useful for cases where you need to process events 
sequentially, for a given key.

go-distr-ep uses redis for persisting events (fault-tolerance) and for distributing 
them among the participating nodes.

## Features
- Events to be processed, for a given key
- Events are processed in order, for a given key
- Events for a given key are processed sequentially (i.e. not concurrently)
- Events can be scheduled to be executed after a delay
- Events are persisted to redis, for fault-tolerance

## Dependencies
- go-redis/v8
- redislock

## Example
```go
type TestCallbackImpl struct {
	callbackName string
}

func (t *TestCallbackImpl) ProcessEvent(key string, val interface{}) {
	// process event..
}


func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	callbackImpl := &TestCallbackImpl{callbackName: name}
	dep := &distr_ep.DistributedEventProcessor{
		RedisClient: client,
		Namespace:   "test1",
		LockTTL:     time.Millisecond * 100,
		CleanupDur:  time.Second * 1,
		Callback:    callbackImpl,
	}
	dep.Init()

	// Produce events
	dep.AddEvent("key1", val, 0)
}

```
## Design
The Distributed Event Processor (DEP) framework's goals are:
- Process events for a _given_ key 
- Events should be processed in-order, for a _given_ key
- Events can be submitted by multiple clients
- Event processing should be resilient (fail-over)

### Components
DEP is designed to be a simple framework with minimal overhead. It uses Redis to store events and to manage distributed locks.

We use `go-redis` and `redis-lock` libraries for Redis client operations and distributed lock, respectively

### Event Submission
When a new event is submitted for a given key
- The event pushed to the end of a redis LIST `{DEP_NS}:evt-ls:{KEY}`
- The TTL of the LIST key is renewed to the configured value
- Key Processor is kicked off

### Key Processor
Key processor attempts to acquire the lock for the specified key and if successful, processes the pending events
- Attempt to acquire redis key lock `{DEP_NS}:pr-lk:{KEY}`
- If key is acquired, process all the pending events for the _key_. Events are stored in the redis LIST `{DEP_NS}:evt-ls:{KEY}`
- For each event in the list:
  - Access the event
  - Invoke the `Callback.ProcessEvent(key, val)`
  - Keep renewing the redis key lock `{DEP_NS}:pr-lk:{KEY}` using a ticker, while the event is being processed
  - When event is processed, POP the element from the redis LIST `{DEP_NS}:evt-ls:{KEY}`
- Release the key lock when processing is done

### Key Lock
The redis key lock is the central piece to achieve sequential processing. It allows us to prevent concurrent execution for the same key. Only one event processor at a time can acquire the lock and thus process events for that key.

As mentioned before, we use the open source library [redislock](https://github.com/bsm/redislock) for this purpose.

### Key Monitor
Key Monitor is a background process whose job is to monitor for any keys that are left unattended. Imagine this scenario:

- `client1` submits an event `evt1` for `key1`
- `client1` starts processing the event since this is the first event for `key1`
- While this is happending, `client2` submits an event `evt2` for `key1`. `evt2` is stored in the redis LIST for `key1`
- `client2` cant acquire the redis lock for `key1` so it cant process the events, so it returns
- Now, let say `client1` **terminates** while in the middle of processing `evt1`

In this case, without a monitor, events for `key1` shall remain in a limbo, _unless_ a new event comes in for that key. Key Monitor is designed to address that short coming by doing a periodic check for any pending keys.

Key Monitor uses redis ZSET to achieve this functionality by optimizing which keys to check for, based on the first expiring keys.

### Scheduled Events (Optional feature)
DEP also supports scheduled events. The Event Scheduler runs a periodic check for scheduled events. It also uses redis ZSET (like the Key Monitor) to monitor for events that are expriring.
