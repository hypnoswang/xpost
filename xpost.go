package xpost

import (
	"log"
	"runtime"
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
			inited:     false,
			started:    false,
			hasSender:  false,
			infoIntv:   0,
			maxProcTo:  0,
			ex:         GetExchanger(),
			mpoolSizes: make(map[int]int),
			mpools:     make(map[int]*MsgPool),
			Couriers:   make(map[string][]Courier),
			stopch:     make(chan struct{}),
			quitch:     make(chan struct{})}
	}

}

func GetXpost() *Xpost {
	return defaltXp
}

type Xpost struct {
	inited     bool
	started    bool
	hasSender  bool
	infoIntv   time.Duration
	maxProcTo  time.Duration
	ex         *Exchanger
	mpoolSizes map[int]int
	mpools     map[int]*MsgPool
	Couriers   map[string][]Courier

	stopch chan struct{}
	quitch chan struct{}
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
		courier := creator.Create()
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
		xp.hasSender = xp.hasSender || courier.IsSender()

		procto := courier.GetProcessTimeout()
		if procto > xp.maxProcTo {
			xp.maxProcTo = procto
		}

		rto := courier.GetWaitTimeout()
		wto := courier.GetPostTimeout()

		if !xp.ex.WireExist(name) {
			cap := courier.GetWireCap()
			if cap < 0 {
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

func (xp *Xpost) initXpost() bool {
	if !xp.hasSender {
		log.Printf("The xpost must have at least one sender")
		return false
	}

	if xp.inited {
		return true
	}

	for poolT := 0; poolT < numOfXpostIf; poolT++ {
		max := xp.GetMsgPoolSize(poolT)
		if max <= 0 {
			log.Printf("MsgPool %s's size is not set\n", xpostIf[poolT])
			return false
		}
	}

	if !xp.initMPools() {
		log.Println("Inilialize MsgPools failed")
		return false
	}

	xp.inited = true

	return true
}

func (xp *Xpost) Deliver(msg *Message) error {
	return xp.ex.Deliver(msg)
}

func (xp *Xpost) start() bool {
	if xp.started {
		return true
	}

	rv := true

Loop:
	for name, couriers := range xp.Couriers {
		log.Printf("Starting courier %s ...\n", name)

		for _, courier := range couriers {
			log.Printf("Starting %s.%d...\n", courier.GetName(), courier.GetId())

			rv = rv && courier.Start()
			if !rv {
				log.Printf("%s.%d start failed!!\n", courier.GetName(), courier.GetId())
				break Loop
			}

			log.Printf("%s.%d started!!\n", courier.GetName(), courier.GetId())
		}

		log.Printf("Courier %s starting finished!!\n", name)
	}

	xp.started = rv

	return rv
}

func (xp *Xpost) Run() {
	if !xp.inited && !xp.initXpost() {
		log.Fatalln("The Xpost Initialization failed") // will call os.Exit(1)
	}

	if !xp.started && !xp.start() {
		log.Fatalln("The Xpost start failed") // will call os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	for name, couriers := range xp.Couriers {
		log.Printf("Running courier %s ...\n", name)

		for _, courier := range couriers {
			log.Printf("Running %s.%d...\n", courier.GetName(), courier.GetId())

			go func(courier Courier) {
				for {
					if courier.IsSender() {
						select {
						case <-xp.stopch:
							log.Printf("Sender Courier %s.%d exit...", courier.GetName(), courier.GetId())
							return
						case <-xp.quitch:
							log.Printf("Courier %s.%d exit...", courier.GetName(), courier.GetId())
							return
						default:
							run(courier)
						}
					} else {
						select {
						case <-xp.quitch:
							log.Printf("Courier %s.%d exit...", courier.GetName(), courier.GetId())
							return
						default:
							run(courier)
						}
					}

				}
			}(courier)
		}
	}

	if xp.infoIntv > 0 {
		go func() {
			ticker := time.Tick(xp.infoIntv * time.Millisecond)
			for {
				select {
				case <-xp.quitch:
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

func (xp *Xpost) stopSendersAndWait(t time.Duration) {
	close(xp.stopch)

	// wait for the goroutines handle the signal
	time.Sleep(t * time.Millisecond)

	for !xp.ex.isClean() {
		time.Sleep(100 * time.Millisecond)
		log.Println("Waiting for all the wires to be clean...")
	}
}

func (xp *Xpost) Stop() {
	t1, t2 := xp.ex.GetMaxTimeout()
	t3 := xp.GetMaxProcessTimeout()
	t := t1 + t2 + t3
	if t < 1000 {
		t = 1000
	}

	// first, stop all the sender couriers and wait ntil all the wires are empty
	xp.stopSendersAndWait(t)

	// second, stop new event generation
	close(xp.quitch)
	// wait for the goroutines handle the signal
	time.Sleep(t * time.Millisecond)

	// third, stop all mpools
	for poolT := 0; poolT < numOfXpostIf; poolT++ {
		xp.mpools[poolT].Stop()
	}

	// forth, stop all courier instances
	for name, couriers := range xp.Couriers {
		log.Printf("Stopping courier %s ...\n", name)
		for _, courier := range couriers {
			if courier.IsSender() {
				continue
			}

			log.Printf("Stopping %s.%d...\n", courier.GetName(), courier.GetId())
			courier.Stop()
			log.Printf("%s.%d stopped!!\n", courier.GetName(), courier.GetId())
		}
	}
}
