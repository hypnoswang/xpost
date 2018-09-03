package xpost

import (
	"sync"
	"time"
)

const (
	sigTimeout = iota
	sigDone
)

// Job interface represents an executable object
type Job interface {
	Run()
}

type worker struct {
	jobch chan Job
	done  chan struct{}
	pool  *Pool
}

// Pool is the collection of goroutines
// it can accept a Job from user and execute it in one worker
type Pool struct {
	name string
	max  int

	lock    *sync.Mutex
	workers []*worker

	freelist chan *worker
	quit     chan struct{}
}

func (w *worker) free() {
	w.pool.freelist <- w
}

func (w *worker) start() {
	logInfof("Start Worker of Pool %s...", w.pool.name)
	go func() {
		for {
			select {
			case job := <-w.jobch:
				w.exec(job)
				w.free()
			case <-w.pool.quit:
				logInfof("Stop Worker of Pool %s...", w.pool.name)
				return
			}
		}
	}()
}

func (w *worker) exec(job Job) {
	defer func() {
		w.done <- struct{}{}
		if e := recover(); e != nil {
			logInfoln(e)
		}
	}()

	job.Run()

}

func (w *worker) dispatch(job Job) {
	if job == nil {
		w.done <- struct{}{}
		return
	}

	w.jobch <- job
}

// NewPool create a new pool instance
func NewPool(name string, max int) *Pool {
	if len(name) <= 0 || max <= 0 {
		return nil
	}

	return &Pool{
		max:      max,
		name:     name,
		lock:     &sync.Mutex{},
		workers:  make([]*worker, 0),
		quit:     make(chan struct{}),
		freelist: make(chan *worker, max),
	}
}

// SetMaxWorker set the max number of workers this pool can create
func (p *Pool) SetMaxWorker(n int) {
	p.max = n
}

// GetMaxWorker returns the max number of workers this pool can create
func (p *Pool) GetMaxWorker() int {
	return p.max
}

func (p *Pool) newWorker() *worker {
	w := &worker{
		jobch: make(chan Job),
		done:  make(chan struct{}),
		pool:  p}

	w.start()

	return w
}

func (p *Pool) increaseWorker() *worker {
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

func (p *Pool) getFreeWorker() *worker {
	var w *worker

	t := time.Millisecond
	for {
		select {
		case w = <-p.freelist:
		default:
			w = p.increaseWorker()
		}

		if w == nil {
			logErrorf("Not enough workers for Pool %s", p.name)
			time.Sleep(t)
			t *= 2
		} else {
			break
		}
	}

	return w
}

// Dispatch dispatch a job to a worker
// it returns an channel which will become readale when the job is finished
func (p *Pool) Dispatch(job Job) <-chan struct{} {
	w := p.getFreeWorker()

	w.dispatch(job)

	return w.done
}

// Info returns the current stats of the pool
func (p *Pool) Info() {
	logInfof(">>>>>> Pool %s info: ", p.name)

	p.lock.Lock()
	created := len(p.workers)
	free := len(p.freelist)
	p.lock.Unlock()

	logInfof(">>>>>> \tname: %s", p.name)
	logInfof(">>>>>> \tmax: %d", p.max)
	logInfof(">>>>>> \tcreated: %d", created)
	logInfof(">>>>>> \tfree: %d", free)
}

// Stop notify all the workers to quit
func (p *Pool) Stop() {
	logInfof("Stop Pool %s...", p.name)

	close(p.quit)
}
