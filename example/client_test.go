package example

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	distr_ep "github.com/pgujjeti/go-distr-ep"
	log "github.com/sirupsen/logrus"
)

type TestCallbackImpl struct {
	callbackName string
}

func (t *TestCallbackImpl) ProcessEvent(key string, val interface{}) {
	// process event
	log.Infof("(%s) %s : processing event : %+v", t.callbackName, key, val)
	time.Sleep(10 * time.Millisecond)
}

func TestRun1(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	log.Info("Running test")
	no_clients, no_msgs := 10, 10
	msg_delay := time.Millisecond * 300
	for i := 1; i <= no_clients; i++ {
		cname := fmt.Sprintf("client%v", i)
		go startClient(cname, no_msgs, msg_delay)
	}
	time.Sleep((time.Second * 10) + (time.Duration(no_msgs) * msg_delay))
}

func startClient(name string, no_msgs int, msg_delay time.Duration) {
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
	for i := 1; i <= no_msgs; i++ {
		val := fmt.Sprintf("%s-value-%v", name, i)
		dep.AddEvent("key1", val)
		if msg_delay > 0 {
			time.Sleep(msg_delay)
		}
	}
	time.Sleep(time.Second * 10)
}
