package example

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	distr_ep "github.com/pgujjeti/go-distr-ep"
	log "github.com/sirupsen/logrus"
)

type testMsg struct {
	key   string
	val   string
	start bool
}

type ConcClient struct {
	dep    *distr_ep.DistributedEventProcessor
	name   string
	msg_ch chan *testMsg
}

type MsgProducer struct {
	name       string
	msg_ch     chan *testMsg
	key        string
	no_of_msgs int
	msg_delay  time.Duration
}

func TestConcurrent1(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02T15:04:05.999999Z07:00",
	})
	log.Info("Running test1")
	no_clients, no_producers, no_msgs := 8, 100, 10
	msg_delay := time.Millisecond * 400
	msg_ch := make(chan *testMsg)

	cc_list := make([]*ConcClient, 0)
	prod_list := make([]*MsgProducer, 0)

	for i := 1; i <= no_clients; i++ {
		cname := fmt.Sprintf("client %v", i)
		cc := createConcurrentClient(cname, msg_ch)
		cc_list = append(cc_list, cc)
	}
	for i := 1; i <= no_producers; i++ {
		name := fmt.Sprintf("producer %v", i)
		p := &MsgProducer{
			name:       name,
			key:        uuid.NewString(),
			no_of_msgs: no_msgs,
			msg_delay:  msg_delay,
			msg_ch:     msg_ch,
		}
		go p.produce()
		prod_list = append(prod_list, p)
	}
	time.Sleep((time.Second * 10) + (time.Duration(no_msgs) * msg_delay))
	// close the clients & producers
	for _, cc := range cc_list {
		cc.close()
	}
	for _, p := range prod_list {
		p.close()
	}
}

func createConcurrentClient(name string, msg_ch chan *testMsg) (c *ConcClient) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"localhost:7000"},
		PoolSize: 2,
		// PoolTimeout: 11 * time.Second,
	})

	c = &ConcClient{
		name:   name,
		msg_ch: msg_ch,
	}
	dep := &distr_ep.DistributedEventProcessor{
		RedisClient: client,
		Namespace:   "test1",
		// LockTTL:     time.Second * 3600,
		CleanupDur: time.Second * 2,
		Callback:   c,
		LogLevel:   log.DebugLevel,
		// AtLeastOnce: true,
		Scheduling: true,
	}
	c.dep = dep
	if err := dep.Init(); err != nil {
		log.Errorf("error initializing.. %v", err)
		return
	}
	go c.readMsgs()
	return c
}

func (c *ConcClient) ProcessEvent(key string, val interface{}, start bool) bool {
	if start {
		log.Infof("Start Processing %s", key)
	}
	// process event
	log.Infof("(%s) %s : processing event : %+v", c.name, key, val)
	time.Sleep(10 * time.Millisecond)
	// var tm TestMessage
	// json.Unmarshal([]byte(val.(string)), &tm)
	// return tm.End
	return false
}

func (c *ConcClient) readMsgs() {
	for tm := range c.msg_ch {
		e := &distr_ep.DistrEvent{Key: tm.key, Val: tm.val}
		log.Infof("Queueing event: %s", e.Val)
		c.dep.AddEvent(e)
	}
}

func (c *ConcClient) close() {
	if c.dep == nil {
		return
	}
	log.Infof("Shutting down client %s..", c.name)
	c.dep.Shutdown()
	c.dep.RedisClient.Close()
}

func (p *MsgProducer) produce() {
	// Produce events
	for ctr := 1; ctr <= p.no_of_msgs; ctr++ {
		m := &testMsg{
			key:   p.key,
			val:   fmt.Sprintf("msg::%s::%d", p.key, ctr),
			start: (ctr == 1),
		}
		log.Infof("%s : adding message to channel: %s", m.key, m.val)
		p.msg_ch <- m
		time.Sleep(time.Duration(rand.Intn(20)) * p.msg_delay / 20)
	}
}

func (p *MsgProducer) close() {
	log.Infof("Closing producer %s...", p.name)
}
