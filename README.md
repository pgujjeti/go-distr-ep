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
	dep.AddEvent("key1", val, time.Second*2)
}

```