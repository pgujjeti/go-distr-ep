package distr_ep

import (
	"time"

	"github.com/rs/xid"
)

// periodic polling job
type pollingTask struct {
	id  string
	dur time.Duration
	ch  chan bool
	job pollJob
}

type pollJob interface {
	runPeriodicJob()
	pollEnded()
}

func (p *pollingTask) start() {
	p.id = xid.New().String()
	p.ch = make(chan bool, 1)
	go p.startPolling()
}

func (p *pollingTask) startPolling() {
	defer p.job.pollEnded()
	ticker := time.NewTicker(p.dur)
	for {
		select {
		case <-ticker.C:
			dlog.Tracef("%s : running job", p.id)
			p.job.runPeriodicJob()
		case <-p.ch:
			dlog.Infof("%s : terminating polling...", p.id)
			return
		}
	}
}

func (p *pollingTask) stop() {
	p.ch <- true
}
