package example

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	distr_ep "github.com/pgujjeti/go-distr-ep"
	log "github.com/sirupsen/logrus"
)

const (
	SESSION_ID_KEY = "key1"
)

type TestCallbackImpl struct {
	callbackName string
}

func (t *TestCallbackImpl) ProcessEvent(key string, val interface{}) {
	// process event
	log.Infof("(%s) %s : processing event : %+v", t.callbackName, key, val)
	time.Sleep(10 * time.Millisecond)
}

type TestMessage struct {
	Key string
	Val string
}

func TestRun1(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	log.Info("Running test")
	no_clients, no_msgs := 1, 10
	msg_delay := time.Millisecond * 400
	for i := 1; i <= no_clients; i++ {
		cname := fmt.Sprintf("client%v", i)
		go startClient(cname, no_msgs, msg_delay)
	}
	time.Sleep((time.Second * 10) + (time.Duration(no_msgs) * msg_delay))
}

func startClient(name string, no_msgs int, msg_delay time.Duration) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"localhost:7000"},
	})
	defer client.Close()

	callbackImpl := &TestCallbackImpl{callbackName: name}
	dep := &distr_ep.DistributedEventProcessor{
		RedisClient: client,
		Namespace:   "test1",
		LockTTL:     time.Millisecond * 1000,
		CleanupDur:  time.Second * 1,
		Callback:    callbackImpl,
		LogLevel:    log.ErrorLevel,
		Scheduling:  true,
	}
	dep.Init()

	// Produce events
	start_ctr := 1
	produceMessages(dep, name, start_ctr, no_msgs, msg_delay)
	start_ctr += no_msgs
	msg := createMessage(name, start_ctr)
	sch_delay := 2 * time.Second
	log.Infof("Scheduling message %s to exec after %v seconds", msg, sch_delay)
	dep.ScheduleEvent(SESSION_ID_KEY, msg, sch_delay)
	start_ctr++
	produceMessages(dep, name, start_ctr, no_msgs, msg_delay)
	time.Sleep(time.Second * 10)
}

func produceMessages(dep *distr_ep.DistributedEventProcessor, name string, start_ctr int, no_msgs int, msg_delay time.Duration) {
	for i := start_ctr; i < (no_msgs + start_ctr); i++ {
		msg := createMessage(name, i)
		dep.AddEvent(SESSION_ID_KEY, msg)
		if msg_delay > 0 {
			time.Sleep(msg_delay)
		}
	}
}

func createMessage(name string, ctr int) string {
	msg := TestMessage{
		Key: fmt.Sprintf("%v", ctr),
		Val: fmt.Sprintf("%s-value-%v", name, ctr),
	}
	val_str, _ := json.Marshal(&msg)
	return string(val_str)
}
