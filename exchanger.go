package xpost

import (
	"errors"
	"log"
)

var g_exchanger *Exchanger

func init() {
	if g_exchanger == nil {
		g_exchanger = &Exchanger{
			xp:    nil,
			wires: make(map[string]*Wire)}
	}
}

func GetExchanger() *Exchanger {
	if g_exchanger == nil {
		g_exchanger = &Exchanger{
			xp:    nil,
			wires: make(map[string]*Wire)}
	}

	return g_exchanger
}

type Wire struct {
	name string
	cap  int
	pipe chan *Message
}

type Exchanger struct {
	xp    *Xpost
	wires map[string]*Wire
}

func NewWire(n string, c int) *Wire {
	if c < 0 || len(n) <= 0 {
		log.Fatalf("Invalid wire attributes: name=%s, cap=%d", n, c)
		return nil
	}

	return &Wire{
		name: n,
		cap:  c,
		pipe: make(chan *Message, c),
	}
}

func (e Exchanger) WireExist(n string) bool {
	_, ok := e.wires[n]

	return ok
}

func (e *Exchanger) SetXpost(xp *Xpost) {
	e.xp = xp
}

func (e *Exchanger) RegisterWire(n string, c int) bool {
	w := NewWire(n, c)
	if w == nil {
		log.Fatal("Could not create new Wire") // will call os.Exit(1)
		return false
	}

	e.wires[n] = w

	return true
}

func (e Exchanger) Info() {
	log.Printf("Wires info:\n")
	for _, wire := range e.wires {
		log.Printf(">>>>>> name: %s\n", wire.name)
		log.Printf(">>>>>> capacity: %d\n", wire.cap)
		log.Printf(">>>>>> msg-queued: %d\n", len(wire.pipe))
		log.Println()
	}
}

type wireDeliverJob struct {
	msg  *Message
	wire *Wire
}

func (wdj *wireDeliverJob) Run() {
	wdj.wire.pipe <- wdj.msg
}

func (e *Exchanger) Deliver(m *Message) error {
	if m == nil {
		return nil
	}

	wires := make([]*Wire, 0)
	for _, dest := range m.dest {
		if w, ok := e.wires[dest]; !ok {
			log.Printf("Deliver to not exist wire: %s", dest)
			return errors.New("Wire not found")
		} else {
			wires = append(wires, w)
		}
	}

	donechs := make([]<-chan struct{}, 0)
	for _, w := range wires {
		wdj := &wireDeliverJob{msg: m, wire: w}
		donech := e.xp.pool.Dispatch(wdj)
		donechs = append(donechs, donech)
	}

	for _, donech := range donechs {
		<-donech
	}

	return nil

}

func (e *Exchanger) Wait(n string) *Message {
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
			log.Printf("Wire %s is not clean: %d", w.name, l)
			return false
		}
	}

	return true
}
