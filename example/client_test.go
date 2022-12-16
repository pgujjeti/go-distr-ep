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

func (t *TestCallbackImpl) StartProcessing(key string) {
	log.Infof("Start Processing %s", key)
}

func (t *TestCallbackImpl) ProcessEvent(key string, val interface{}) bool {
	// process event
	log.Infof("(%s) %s : processing event : %+v", t.callbackName, key, val)
	time.Sleep(10 * time.Millisecond)
	var tm TestMessage
	json.Unmarshal([]byte(val.(string)), &tm)
	return tm.End
}

type TestMessage struct {
	Key string
	Val string
	End bool
}

func TestRun1(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	log.Info("Running test")
	no_clients, no_msgs := 10, 10
	msg_delay := time.Millisecond * 400
	for i := 1; i <= no_clients; i++ {
		cname := fmt.Sprintf("client %v", i)
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
		// LockTTL:     time.Second * 1000,
		CleanupDur:  time.Second * 1,
		Callback:    callbackImpl,
		LogLevel:    log.InfoLevel,
		AtLeastOnce: true,
		Scheduling:  true,
	}
	if err := dep.Init(); err != nil {
		log.Errorf("error initializing.. %v", err)
		return
	}

	// Produce events
	start_ctr := 1
	produceMessages(dep, name, start_ctr, no_msgs, msg_delay)
	start_ctr += no_msgs
	msg := createMessage(name, start_ctr, false)
	sch_delay := 2 * time.Second
	log.Infof("Scheduling message %s to exec after %v seconds", msg, sch_delay)
	e := &distr_ep.DistrEvent{Key: SESSION_ID_KEY, Val: msg}
	dep.ScheduleEvent(e, sch_delay)
	start_ctr++
	produceMessages(dep, name, start_ctr, no_msgs, msg_delay)
	start_ctr += no_msgs
	lastMessage(dep, name, start_ctr)
	start_ctr++
	time.Sleep(time.Second * 10)
}

func produceMessages(dep *distr_ep.DistributedEventProcessor, name string, start_ctr int, no_msgs int, msg_delay time.Duration) {
	for i := start_ctr; i < (no_msgs + start_ctr); i++ {
		var start bool
		if i == 1 {
			start = true
		}
		msg := createMessage(name, i, false)
		e := &distr_ep.DistrEvent{Key: SESSION_ID_KEY, Val: msg, Start: start}
		queueEvent(dep, e)
		if msg_delay > 0 {
			time.Sleep(msg_delay)
		}
	}
}

func lastMessage(dep *distr_ep.DistributedEventProcessor, name string, ctr int) {
	msg := createMessage(name, ctr, true)
	e := &distr_ep.DistrEvent{Key: SESSION_ID_KEY, Val: msg}
	queueEvent(dep, e)
}

func queueEvent(dep *distr_ep.DistributedEventProcessor, e *distr_ep.DistrEvent) {
	log.Infof("Queueing event: %s", e.Val)
	dep.AddEvent(e)
}

func createMessage(name string, ctr int, end bool) string {
	msg := TestMessage{
		Key: fmt.Sprintf("%v", ctr),
		Val: fmt.Sprintf("%s-value-%v", name, ctr),
		End: end,
	}
	val_str, _ := json.Marshal(&msg)
	return string(val_str)
}
