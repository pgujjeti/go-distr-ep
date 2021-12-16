package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	distr_ep "github.com/pgujjeti/go-distr-ep"
	log "github.com/sirupsen/logrus"
)

type TestCallbackImpl struct {
}

func (t *TestCallbackImpl) ProcessEvent(key string, val interface{}) {
	// process event
	log.Infof("%s : processing event : %+v", key, val)
}

func TestRun1(t *testing.T) {
	log.Info("Running test")
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	callbackImpl := &TestCallbackImpl{}
	dep := &distr_ep.DistributedEventProcessor{
		RedisClient: client,
		Namespace:   "test1",
		LockTTL:     time.Millisecond * 1000,
		CleanupDur:  time.Second * 5,
		Callback:    callbackImpl,
	}
	dep.Init()

	// Produce events
	for i := 1; i <= 10; i++ {
		val := fmt.Sprintf("value-%v", i)
		dep.AddEvent("key1", val)
	}
	time.Sleep(time.Second * 10)
}
