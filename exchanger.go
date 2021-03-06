package xpost

import (
	"errors"
)

var gExchanger *Exchanger

func init() {
	if gExchanger == nil {
		gExchanger = &Exchanger{
			xp:    nil,
			wires: make(map[string]*wire)}
	}
}

// GetExchanger returns a global default exchanger instance
func GetExchanger() *Exchanger {
	if gExchanger == nil {
		gExchanger = &Exchanger{
			xp:    nil,
			wires: make(map[string]*wire)}
	}

	return gExchanger
}

// wire represents a communication channel for a courier
// multiple instances of the same courier share one wire
type wire struct {
	name string
	cap  int
	pipe chan *Message
}

// Exchanger is a collector of wires
type Exchanger struct {
	xp    *Xpost
	wires map[string]*wire
}

func newWire(n string, c int) *wire {
	if c < 0 || len(n) <= 0 {
		logFatalf("Invalid wire attributes: name=%s, cap=%d", n, c)
		return nil
	}

	return &wire{
		name: n,
		cap:  c,
		pipe: make(chan *Message, c),
	}
}

func (e Exchanger) wireExist(n string) bool {
	_, ok := e.wires[n]

	return ok
}

func (e *Exchanger) setXpost(xp *Xpost) {
	e.xp = xp
}

func (e *Exchanger) registerWire(n string, c int) bool {
	w := newWire(n, c)
	if w == nil {
		logFatal("Could not create new wire") // will call os.Exit(1)
		return false
	}

	e.wires[n] = w

	return true
}

// Info return s the current stats of the exchanger
func (e Exchanger) Info() {
	logInfof("Wires info:")
	for _, wire := range e.wires {
		logInfof(">>>>>> name: %s", wire.name)
		logInfof(">>>>>> capacity: %d", wire.cap)
		logInfof(">>>>>> msg-queued: %d", len(wire.pipe))
	}
}

type wireDeliverJob struct {
	msg  *Message
	wire *wire
}

func (wdj *wireDeliverJob) Run() {
	wdj.wire.pipe <- wdj.msg
}

func (e *Exchanger) deliver(m *Message) error {
	if m == nil {
		return nil
	}

	wires := make([]*wire, 0)
	for _, dest := range m.dest {
		w, ok := e.wires[dest]
		if !ok {
			logErrorf("Deliver to not exist wire: %s", dest)
			return errors.New("Wire not found")
		}
		wires = append(wires, w)
	}

	donechs := make([]<-chan struct{}, 0)
	for idx, w := range wires {
		newMsg := m
		if idx > 0 {
			newMsg = m.Clone()
		}

		wdj := &wireDeliverJob{msg: newMsg, wire: w}
		donech := e.xp.pool.Dispatch(wdj)
		donechs = append(donechs, donech)
	}

	for _, donech := range donechs {
		<-donech
	}

	return nil

}

func (e *Exchanger) wait(n string) *Message {
	w, ok := e.wires[n]
	if !ok {
		return nil
	}

	msg := <-w.pipe

	return msg
}

func (e *Exchanger) isClean() bool {
	for _, w := range e.wires {
		if l := len(w.pipe); l > 0 {
			logErrorf("Wire %s is not clean: %d", w.name, l)
			return false
		}
	}

	return true
}
