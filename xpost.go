package xpost

import (
	"context"
	"log"
	"time"
)

var defaltXp *Xpost

const (
	WaitT = iota
	ProcessT
	PostT
	numOfXpostIf
)

var xpostIf = []string{
	"wait",
	"process",
	"post"}

func init() {
	if defaltXp == nil {
		defaltXp = &Xpost{
			infoIntv:   0,
			inited:     false,
			maxProcTo:  0,
			ex:         GetExchanger(),
			mpoolSizes: make(map[int]int),
			mpools:     make(map[int]*MsgPool),
			Couriers:   make(map[string][]Courier)}
	}

}

func GetXpost() *Xpost {
	return defaltXp
}

type Xpost struct {
	infoIntv   time.Duration
	inited     bool
	canceller  context.CancelFunc
	maxProcTo  time.Duration
	ex         *Exchanger
	mpoolSizes map[int]int
	mpools     map[int]*MsgPool
	Couriers   map[string][]Courier
}

func (xp *Xpost) RegiserExchanger(e *Exchanger) bool {
	if nil == e {
		return false
	}

	xp.ex = e

	return true
}

func (xp *Xpost) RegisterCourier(creator CourierCreator, concurrency int) bool {

	for i := 0; i < concurrency; i++ {
		courier := creator()
		if courier == nil {
			return false
		}

		name := courier.GetName()
		if len(name) <= 0 {
			return false
		}

		courier.setXpost(xp)
		courier.setId(i)
		xp.Couriers[name] = append(xp.Couriers[name], courier)

		procto := courier.GetProcessTimeout()
		if procto > xp.maxProcTo {
			xp.maxProcTo = procto
		}

		rto := courier.GetWaitTimeout()
		if rto <= 0 {
			rto = 1000
		}

		wto := courier.GetPostTimeout()
		if wto <= 0 {
			wto = 1000
		}

		if !xp.ex.WireExist(name) {
			cap := courier.GetWireCap()
			if cap <= 0 {
				return false
			}

			if !xp.ex.RegisterWire(name, cap, rto, wto) {
				return false
			}
		}
	}

	return true
}

func (xp *Xpost) GetMaxProcessTimeout() time.Duration {
	return xp.maxProcTo
}

func (xp *Xpost) GetDumpInfoInterval() time.Duration {
	return xp.infoIntv
}

func (xp *Xpost) SetDumpInfoInterval(n time.Duration) {
	xp.infoIntv = n
}

func (xp *Xpost) GetMsgPoolSize(poolT int) int {
	if n, ok := xp.mpoolSizes[poolT]; ok {
		return n
	}

	return 0
}

func (xp *Xpost) SetMsgPoolSize(poolT int, n int) {
	if poolT < 0 || poolT >= numOfXpostIf {
		log.Fatalf("Unsupported poolT: %d\n", poolT)
	}

	xp.mpoolSizes[poolT] = n
}

func (xp *Xpost) initMPools() bool {
	for poolT := 0; poolT < numOfXpostIf; poolT++ {
		max := xp.GetMsgPoolSize(poolT)
		mp := NewMsgPool(poolT, max, xp)
		if mp == nil {
			return false
		}

		mp.SetMaxWorker(max)
		xp.mpools[poolT] = mp
	}

	return true
}

func (xp *Xpost) Init() bool {
	if xp.inited {
		return true
	}

	for poolT := 0; poolT < numOfXpostIf; poolT++ {
		max := xp.GetMsgPoolSize(poolT)
		if max <= 0 {
			log.Fatalf("MsgPool %s's size is not set\n", xpostIf[poolT]) // will call os.Exit(1)
		}
	}

	if !xp.initMPools() {
		log.Fatalln("Inilialize MsgPools failed") // will call os.Exit(1)
	}

	xp.inited = true

	return true
}

func (xp *Xpost) Run() {
	if !xp.inited {
		log.Fatalln("The Xpost has not been initialized") // will call os.Exit(1)
	}

	ctx, canceller := context.WithCancel(context.Background())
	xp.canceller = canceller

	for name, couriers := range xp.Couriers {
		log.Printf("Starting courier %s ...\n", name)

		for _, courier := range couriers {
			log.Printf("Starting %s.%d...\n", courier.GetName(), courier.GetId())

			go func(courier Courier) {
				for {
					select {
					case <-ctx.Done():
						log.Printf("Courier %s.%d exit...", courier.GetName(), courier.GetId())
						return
					default:
						run(courier)
					}

				}
			}(courier)
		}

		log.Printf("Courier %s starting finished!!\n", name)
	}

	if xp.infoIntv > 0 {
		go func() {
			ticker := time.Tick(xp.infoIntv * time.Millisecond)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker:
					xp.Info()
				}
			}
		}()
	}
}

func (xp *Xpost) Info() {
	log.Printf(">>>>>> Couriers info:\n")
	for _, couriers := range xp.Couriers {
		for _, courier := range couriers {
			log.Printf(">>>>>> id: %d\n", courier.GetId())
			log.Printf(">>>>>> name: %s\n", courier.GetName())
			log.Printf(">>>>>> wirecap: %d\n", courier.GetWireCap())
			log.Printf(">>>>>> WaitTimeout: %d\n", courier.GetWaitTimeout())
			log.Printf(">>>>>> ProcTimeout: %d\n", courier.GetProcessTimeout())
			log.Printf(">>>>>> PostTimeout: %d\n", courier.GetPostTimeout())
			log.Println()
		}
	}

	log.Printf("\n\n")

	xp.ex.Info()

	log.Printf("\n\n")

	log.Printf(">>>>>> MsgPools info:\n")
	for _, mpool := range xp.mpools {
		mpool.Info()
	}
}

func (xp *Xpost) Stop() {
	// first, stop new event generation
	xp.canceller()

	// second, stop all mpools
	for poolT := 0; poolT < numOfXpostIf; poolT++ {
		xp.mpools[poolT].Stop()
	}

	// wait for the last dispatched event to finish
	t1, t2 := xp.ex.GetMaxTimeout()
	t3 := xp.GetMaxProcessTimeout()
	t := t1 + t2 + t3
	if t < 3000 {
		t = 3000
	}

	time.Sleep(t * time.Millisecond)
}