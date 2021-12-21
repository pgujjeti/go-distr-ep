# go-distr-ep
This project provides a distributed event processor framework for golang. It 
uses redis for persisting the events and distributing them among the 
participating nodes.

## Features
- Submit events to be processed
- Framework guarantees events are processed in order, for a given key
- Events for a given key are processed sequentially (not concurrently)
- Events can be scheduled to be executed after a delay

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