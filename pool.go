package xpost

import (
	"log"
	"sync"
	"time"
)

const (
	sigTimeout = iota
	sigDone
)

type Job interface {
	Run()
}

type Worker struct {
	jobch chan Job
	done  chan struct{}
	pool  *Pool
}

type Pool struct {
	name string
	max  int

	xp      *Xpost
	lock    *sync.Mutex
	workers []*Worker

	freelist chan *Worker
	quit     chan struct{}
}

func (w *Worker) free() {
	w.pool.freelist <- w
}

func (w *Worker) start() {
	log.Printf("Start Worker of Pool %s...", w.pool.name)
	go func() {
		for {
			select {
			case job := <-w.jobch:
				w.exec(job)
				w.free()
			case <-w.pool.quit:
				log.Printf("Stop Worker of Pool %s...\n", w.pool.name)
				return
			}
		}
	}()
}

func (w *Worker) exec(job Job) {
	defer func() {
		w.done <- struct{}{}
		if e := recover(); e != nil {
			log.Println(e)
		}
	}()

	job.Run()

}

func (w *Worker) dispatch(job Job) {
	if job == nil {
		w.done <- struct{}{}
		return
	}

	w.jobch <- job
}

func NewPool(name string, max int, xp *Xpost) *Pool {
	if len(name) <= 0 || max <= 0 || xp == nil {
		return nil
	}

	return &Pool{
		max:      max,
		name:     name,
		xp:       xp,
		lock:     &sync.Mutex{},
		workers:  make([]*Worker, 0),
		quit:     make(chan struct{}),
		freelist: make(chan *Worker, max),
	}
}

func (p *Pool) SetMaxWorker(n int) {
	p.max = n
}

func (p *Pool) GetMaxWorker() int {
	return p.max
}

func (p *Pool) newWorker() *Worker {
	w := &Worker{
		jobch: make(chan Job),
		done:  make(chan struct{}),
		pool:  p}

	w.start()

	return w
}

func (p *Pool) increaseWorker() *Worker {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.workers) >= p.max {
		return nil
	}

	w := p.newWorker()
	if nil == w {
		return nil
	}

	p.workers = append(p.workers, w)

	return w
}

func (p *Pool) getFreeWorker() *Worker {
	var w *Worker = nil

	t := time.Millisecond
	for {
		select {
		case w = <-p.freelist:
		default:
			w = p.increaseWorker()
		}

		if w == nil {
			log.Printf("Not enough workers for Pool %s\n", p.name)
			time.Sleep(t)
			t *= 2
		} else {
			break
		}
	}

	return w
}

func (p *Pool) Dispatch(job Job) <-chan struct{} {
	w := p.getFreeWorker()

	w.dispatch(job)

	return w.done
}

func (p *Pool) Info() {
	log.Printf(">>>>>> Pool %s info: \n", p.name)

	p.lock.Lock()
	created := len(p.workers)
	free := len(p.freelist)
	p.lock.Unlock()

	log.Printf(">>>>>> \tname: %s\n", p.name)
	log.Printf(">>>>>> \tmax: %d\n", p.max)
	log.Printf(">>>>>> \tcreated: %d\n", created)
	log.Printf(">>>>>> \tfree: %d\n", free)
}

func (p *Pool) Stop() {
	log.Printf("Stop Pool %s...\n", p.name)

	close(p.quit)
}
