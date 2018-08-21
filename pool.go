package xpost

import (
	"context"
	"log"
	"sync"
	"time"
)

const (
	sigTimeout = iota
	sigDone
)

type MsgJob struct {
	jobT    int
	courier Courier
	msg     *Message
}

type MsgWorker struct {
	ch     chan *MsgJob
	notify chan int
	done   chan *Message
	pool   *MsgPool
}

type MsgPool struct {
	max      int
	poolT    int
	xp       *Xpost
	lock     *sync.Mutex
	workers  []*MsgWorker
	freelist chan *MsgWorker
	quit     chan struct{}
}

func NewMsgJob(t int, c Courier, m *Message) *MsgJob {
	job := &MsgJob{
		jobT:    t,
		courier: c,
		msg:     m}

	if !job.Validate() {
		return nil
	}

	return job
}

func (mj *MsgJob) Validate() bool {
	if mj.courier == nil ||
		(mj.jobT != WaitT && mj.msg == nil) {
		return false
	}

	if mj.jobT < 0 || mj.jobT >= numOfXpostIf {
		return false
	}

	return true
}

func (mw *MsgWorker) free() {
	mw.pool.freelist <- mw
}

func (mw *MsgWorker) start() {
	log.Printf("Start MsgWorker of MsgPool.%d...", mw.pool.poolT)
	go func() {
		for {
			select {
			case job := <-mw.ch:
				mw.exec(job)
			case <-mw.pool.quit:
				log.Printf("Stop MsgWorker of MsgPool.%d...\n", mw.pool.poolT)
				return
			case sig := <-mw.notify:
				switch sig {
				case sigTimeout:
					mw.done = make(chan *Message, 1)
					fallthrough
				case sigDone:
					mw.free()
				default:
					//do nothing
				}
			}
		}
	}()
}

func (mw *MsgWorker) exec(job *MsgJob) {
	courier := job.courier
	msg := job.msg

	var rmsg *Message = nil
	switch job.jobT {
	case WaitT:
		rmsg = courier.Wait()
	case ProcessT:
		rmsg = courier.Process(msg)
	case PostT:
		rmsg = courier.Post(msg)
	}

	mw.done <- rmsg
}

func (mw *MsgWorker) dispatch(job *MsgJob) *Message {
	if job == nil || job.jobT != mw.pool.poolT {
		mw.free()
		return nil
	}

	mw.ch <- job

	var to time.Duration = 0 //default value
	switch job.jobT {
	case WaitT:
		to = job.courier.GetWaitTimeout()
	case ProcessT:
		to = job.courier.GetProcessTimeout()
	case PostT:
		to = job.courier.GetPostTimeout()
	}

	if to > 0 {
		ctx, canceller := context.WithTimeout(context.Background(), to*time.Millisecond)
		defer canceller()

		select {
		case <-ctx.Done():
			//notify the worker the timeout event
			mw.notify <- sigTimeout
			return nil
		case msg := <-mw.done:
			mw.notify <- sigDone
			return msg
		}
	} else {
		msg := <-mw.done
		mw.notify <- sigDone
		return msg
	}
}

func NewMsgPool(poolT int, max int, xp *Xpost) *MsgPool {
	if poolT < 0 || poolT >= numOfXpostIf {
		return nil
	}

	if max <= 0 || xp == nil {
		return nil
	}

	return &MsgPool{
		freelist: make(chan *MsgWorker, max),
		max:      max,
		poolT:    poolT,
		workers:  make([]*MsgWorker, 0),
		xp:       xp,
		lock:     &sync.Mutex{},
		quit:     make(chan struct{}),
	}
}

func (mp *MsgPool) SetMaxWorker(n int) {
	mp.max = n
}

func (mp *MsgPool) GetMaxWorker() int {
	return mp.max
}

func (mp *MsgPool) newMsgWorker() *MsgWorker {
	mw := &MsgWorker{
		ch:     make(chan *MsgJob, 1),
		done:   make(chan *Message, 1),
		notify: make(chan int, 1),
		pool:   mp}

	mw.start()

	return mw
}

func (mp *MsgPool) increaseWorker() *MsgWorker {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	if len(mp.workers) >= mp.max {
		return nil
	}

	mw := mp.newMsgWorker()
	if nil == mw {
		return nil
	}

	mp.workers = append(mp.workers, mw)

	return mw
}

func (mp *MsgPool) getFreeWorker() *MsgWorker {
	var mw *MsgWorker = nil

	t := time.Millisecond
	for {
		select {
		case mw = <-mp.freelist:
		default:
			mw = mp.increaseWorker()
		}

		if mw == nil {
			log.Printf("Not enough workers for MsgPool.%d\n", mp.poolT)
			time.Sleep(t)
			t *= 2
		} else {
			break
		}
	}

	return mw
}

func (mp *MsgPool) Dispatch(job *MsgJob) *Message {
	mw := mp.getFreeWorker()

	return mw.dispatch(job)
}

func (mp *MsgPool) Info() {
	log.Printf(">>>>>> Type\t\tMax\t\tCreated\t\tFree\n")
	log.Printf(">>>>>> __________________________________________________________\n")

	mp.lock.Lock()
	created := len(mp.workers)
	free := len(mp.freelist)
	mp.lock.Unlock()

	log.Printf(">>>>>> %s\t\t%d\t\t%d\t\t%d\n\n", xpostIf[mp.poolT], mp.max, created, free)
}

func (mp *MsgPool) Stop() {
	log.Printf("Stop MsgPool %d...\n", mp.poolT)

	close(mp.quit)
}
