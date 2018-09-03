package xpost

import (
	"runtime"
	"time"

	"github.com/hypnoswang/xlog"
)

var logger xlog.Logger
var defaltXp *Xpost

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

	logger = xlog.NewConsoleLogger(xlog.InfoLevel, xlog.TextFormatter)
}

// SetLogger set the logger instance of the whole xpost world
func SetLogger(l xlog.Logger) {
	logger = l
}

// GetXpost return the global default Xpost instance
func GetXpost() *Xpost {
	return defaltXp
}

// Xpost represents a collection of couriers which are connected by their wires
// all the wires are managed by the Exchanger
// X means a connected net, post means messages or transition of messages
// couriers are responsible for receiving, proccesing and delivering messages
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

// RegiserExchanger register an exchanger object to xpost
func (xp *Xpost) RegiserExchanger(e *Exchanger) bool {
	if nil == e {
		return false
	}

	xp.ex = e
	e.xp = xp

	return true
}

// RegisterCourier register couriers to the xpost
// we use a creator here so that we can create multiple instances of the user define courier type
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
		courier.setID(i)
		xp.couriers[name] = append(xp.couriers[name], courier)
		xp.hasSender = xp.hasSender || courier.IsSender()

		if !xp.ex.wireExist(name) {
			cap := courier.GetWireCap()
			if cap < 0 {
				return false
			}

			if !xp.ex.registerWire(name, cap) {
				return false
			}
		}
	}

	return true
}

// GetDumpInfoInterval returns the time interval for dump the stats of xpost world
func (xp *Xpost) GetDumpInfoInterval() time.Duration {
	return xp.infoIntv
}

// SetDumpInfoInterval sets the time interval for dump the stats of xpost world
// 0 means never dump the infomation
func (xp *Xpost) SetDumpInfoInterval(n time.Duration) {
	xp.infoIntv = n
}

// SetPoolSize sets the max number of workers of the underlying pool
func (xp *Xpost) SetPoolSize(n int) {
	xp.poolSize = n
}

// GetPoolSize returns the max number of workers of the underlying pool
func (xp *Xpost) GetPoolSize() int {
	return xp.poolSize
}

// SetPoolName sets the name of the underlying pool
func (xp *Xpost) SetPoolName(s string) {
	xp.poolName = s
}

// GetPoolName returns the name of the underlying pool
func (xp *Xpost) GetPoolName() string {
	return xp.poolName
}

func (xp *Xpost) init() bool {
	if xp.inited {
		return true
	}

	if !xp.hasSender {
		logError("The xpost must have at least one sender")
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

	pool := NewPool(xp.poolName, xp.poolSize)
	if pool == nil {
		logError("Create pool failed")
		return false
	}

	xp.ex.setXpost(xp)
	xp.pool = pool

	xp.inited = true

	return true
}

// Deliver send a message to its destination wires
// we exposed this method so that users can deliver multiple messages during one job.Run()
func (xp *Xpost) Deliver(msg *Message) error {
	return xp.ex.deliver(msg)
}

func (xp *Xpost) start() bool {
	if xp.started {
		return true
	}

	rv := true

Loop:
	for name, couriers := range xp.couriers {
		logInfof("Starting courier %s ...", name)

		for _, courier := range couriers {
			logInfof("Starting %s.%d...", courier.GetName(), courier.GetID())

			rv = rv && courier.Start()
			if !rv {
				logInfof("%s.%d start failed!!", courier.GetName(), courier.GetID())
				break Loop
			}

			logInfof("%s.%d started!!", courier.GetName(), courier.GetID())
		}

		logInfof("Courier %s starting finished!!", name)
	}

	xp.started = rv

	return rv
}

// Run get the xpost to start the work
func (xp *Xpost) Run() {
	if !xp.inited && !xp.init() {
		logFatalln("The Xpost Initialization failed") // will call os.Exit(1)
	}

	if !xp.started && !xp.start() {
		logFatalln("The Xpost start failed") // will call os.Exit(1)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	for name, couriers := range xp.couriers {
		logInfof("Running courier %s ...", name)

		for _, courier := range couriers {
			logInfof("Running %s.%d...", courier.GetName(), courier.GetID())

			go func(courier Courier) {
				job := &courierJob{courier: courier}
				quit := false
				for {
					if quit {
						return
					}

					// The reason we dispatch the job to the pool is:
					// By this way we can respond a quit or senderch event immediately
					// if we call the run(courier) func in this goroutine, the execution may
					// blocked in this run(courier) and the quit/senderch will never get a
					// chance to be handled
					jobdone := xp.pool.Dispatch(job)
					if courier.IsSender() {
						select {
						case <-xp.senderch:
							quit = true
							logInfof("Sender Courier %s.%d exit...", courier.GetName(), courier.GetID())
						case <-xp.quitch:
							logInfof("Sender Courier %s.%d exit...", courier.GetName(), courier.GetID())
							quit = true
						case <-jobdone:
							continue
						}
					} else {
						select {
						case <-xp.quitch:
							logInfof("Courier %s.%d exit...", courier.GetName(), courier.GetID())
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

// Info dumps the stats of the whole xpost world
func (xp *Xpost) Info() {
	logInfof(">>>>>> couriers info:")
	for _, couriers := range xp.couriers {
		for _, courier := range couriers {
			logInfof(">>>>>> id: %d", courier.GetID())
			logInfof(">>>>>> name: %s", courier.GetName())
			logInfof(">>>>>> wirecap: %d", courier.GetWireCap())
			logInfoln()
		}
	}

	logInfof("\n")

	xp.ex.Info()

	logInfof("\n")

	xp.pool.Info()
}

func (xp *Xpost) stopSendersAndWait(t time.Duration) {
	close(xp.senderch)

	// wait for the goroutines handle the signal
	time.Sleep(t * time.Millisecond)

	for !xp.ex.isClean() {
		time.Sleep(100 * time.Millisecond)
		logInfoln("Waiting for all the wires to be clean...")
	}
}

// Stop stops the xpost world
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
		logInfof("Stopping courier %s ...", name)
		for _, courier := range couriers {
			if courier.IsSender() {
				continue
			}

			logInfof("Stopping %s.%d...", courier.GetName(), courier.GetID())
			courier.Stop()
			logInfof("%s.%d stopped!!", courier.GetName(), courier.GetID())
		}
	}
}
