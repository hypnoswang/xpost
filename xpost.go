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
			inited:    false,
			started:   false,
			hasSender: false,
			infoIntv:  0,
			ex:        GetExchanger(),
			pool:      nil,
			couriers:  make(map[string][]Courier),
			senderch:  make(chan struct{}),
			quitch:    make(chan struct{})}
	}

}

func GetXpost() *Xpost {
	return defaltXp
}

type Xpost struct {
	inited    bool
	started   bool
	hasSender bool

	infoIntv time.Duration

	poolSize int
	poolName string
	pool     *Pool

	ex       *Exchanger
	couriers map[string][]Courier

	senderch chan struct{}
	quitch   chan struct{}
}

func (xp *Xpost) RegiserExchanger(e *Exchanger) bool {
	if nil == e {
		return false
	}

	xp.ex = e
	e.xp = xp

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
		xp.couriers[name] = append(xp.couriers[name], courier)
		xp.hasSender = xp.hasSender || courier.IsSender()

		if !xp.ex.WireExist(name) {
			cap := courier.GetWireCap()
			if cap < 0 {
				return false
			}

			if !xp.ex.RegisterWire(name, cap) {
				return false
			}
		}
	}

	return true
}

func (xp *Xpost) GetDumpInfoInterval() time.Duration {
	return xp.infoIntv
}

func (xp *Xpost) SetDumpInfoInterval(n time.Duration) {
	xp.infoIntv = n
}

func (xp *Xpost) SetPoolSize(n int) {
	xp.poolSize = n
}

func (xp *Xpost) GetPoolSize() int {
	return xp.poolSize
}

func (xp *Xpost) SetPoolName(s string) {
	xp.poolName = s
}

func (xp *Xpost) GetPoolName() string {
	return xp.poolName
}

func (xp *Xpost) init() bool {
	if xp.inited {
		return true
	}

	if !xp.hasSender {
		log.Printf("The xpost must have at least one sender")
		return false
	}

	if len(xp.poolName) <= 0 {
		xp.poolName = "DefaultPool"
	}

	if xp.poolSize <= 0 {
		wireCnt := len(xp.ex.wires)

		courierCnt := 0
		for _, couries := range xp.couriers {
			courierCnt += len(couries)
		}

		xp.poolSize = 2 * (wireCnt + courierCnt)
	}

	pool := NewPool(xp.poolName, xp.poolSize, xp)
	if pool == nil {
		log.Printf("Create pool failed")
		return false
	}

	xp.ex.SetXpost(xp)
	xp.pool = pool

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
	for name, couriers := range xp.couriers {
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
	if !xp.inited && !xp.init() {
		log.Fatalln("The Xpost Initialization failed") // will call os.Exit(1)
	}

	if !xp.started && !xp.start() {
		log.Fatalln("The Xpost start failed") // will call os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	for name, couriers := range xp.couriers {
		log.Printf("Running courier %s ...\n", name)

		for _, courier := range couriers {
			log.Printf("Running %s.%d...\n", courier.GetName(), courier.GetId())

			go func(courier Courier) {
				job := &CourierJob{courier: courier}
				quit := false
				for {
					if quit {
						return
					}

					jobdone := xp.pool.Dispatch(job)
					if courier.IsSender() {
						select {
						case <-xp.senderch:
							quit = true
							log.Printf("Sender Courier %s.%d exit...", courier.GetName(), courier.GetId())
						case <-xp.quitch:
							log.Printf("Sender Courier %s.%d exit...", courier.GetName(), courier.GetId())
							quit = true
						case <-jobdone:
							continue
						}
					} else {
						select {
						case <-xp.quitch:
							log.Printf("Courier %s.%d exit...", courier.GetName(), courier.GetId())
							quit = true
						case <-jobdone:
							continue
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
	log.Printf(">>>>>> couriers info:\n")
	for _, couriers := range xp.couriers {
		for _, courier := range couriers {
			log.Printf(">>>>>> id: %d\n", courier.GetId())
			log.Printf(">>>>>> name: %s\n", courier.GetName())
			log.Printf(">>>>>> wirecap: %d\n", courier.GetWireCap())
			log.Println()
		}
	}

	log.Printf("\n\n")

	xp.ex.Info()

	log.Printf("\n\n")

	xp.pool.Info()
}

func (xp *Xpost) stopSendersAndWait(t time.Duration) {
	close(xp.senderch)

	// wait for the goroutines handle the signal
	time.Sleep(t * time.Millisecond)

	for !xp.ex.isClean() {
		time.Sleep(100 * time.Millisecond)
		log.Println("Waiting for all the wires to be clean...")
	}
}

func (xp *Xpost) Stop() {
	// first, stop all the sender couriers and wait ntil all the wires are empty
	xp.stopSendersAndWait(1000)

	// second, stop new event generation
	close(xp.quitch)
	// wait for the goroutines handle the signal
	time.Sleep(1000 * time.Millisecond)

	// third, stop all mpools
	xp.pool.Stop()

	// forth, stop all courier instances
	for name, couriers := range xp.couriers {
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
