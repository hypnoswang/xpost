package xpost

import (
	//"fmt"
	//"context"
	"errors"
	"log"
	"time"
)

var g_exchanger *Exchanger

func init() {
	if g_exchanger == nil {
		g_exchanger = &Exchanger{
			maxRdTo: 0,
			maxWrTo: 0,
			wires:   make(map[string]*Wire)}
	}
}

func GetExchanger() *Exchanger {
	if g_exchanger == nil {
		g_exchanger = &Exchanger{
			maxRdTo: 0,
			maxWrTo: 0,
			wires:   make(map[string]*Wire)}
	}

	return g_exchanger
}

type Wire struct {
	name    string
	cap     int
	readTo  time.Duration
	writeTo time.Duration
	pipe    chan *Message
}

type Exchanger struct {
	maxRdTo time.Duration
	maxWrTo time.Duration
	wires   map[string]*Wire
}

func NewWire(n string, c int, rto, wto time.Duration) *Wire {
	if c <= 0 || rto <= 0 || wto <= 0 {
		log.Fatalf("Invalid wire attributes: name=%s, cap=%d, readTo=%d, writeTo=%d", n, c, rto, wto)
		return nil
	}

	return &Wire{
		name:    n,
		cap:     c,
		readTo:  rto,
		writeTo: wto,
		pipe:    make(chan *Message, c),
	}
}

func (w *Wire) SetReadTimeout(to time.Duration) {
	w.readTo = to
}

func (w *Wire) SetWriteTimeout(to time.Duration) {
	w.writeTo = to
}

func (e Exchanger) WireExist(n string) bool {
	_, ok := e.wires[n]

	return ok
}

func (e *Exchanger) RegisterWire(n string, c int, rto, wto time.Duration) bool {
	w := NewWire(n, c, rto, wto)
	if w == nil {
		log.Fatal("Could not create new Wire") // will call os.Exit(1)
		return false
	}

	if rto > e.maxRdTo {
		e.maxRdTo = rto
	}

	if wto > e.maxWrTo {
		e.maxWrTo = wto
	}

	e.wires[n] = w

	return true
}

func (e Exchanger) Info() {
	log.Printf("Wires info:\n")
	for _, wire := range e.wires {
		log.Printf(">>>>>> name: %s\n", wire.name)
		log.Printf(">>>>>> capacity: %d\n", wire.cap)
		log.Printf(">>>>>> readtimeout: %d\n", wire.readTo)
		log.Printf(">>>>>> writetimeout: %d\n", wire.writeTo)
		log.Printf(">>>>>> msg-queued: %d\n", len(wire.pipe))
		log.Println()
	}
}

func (e Exchanger) GetMaxTimeout() (rto, wto time.Duration) {
	return e.maxRdTo, e.maxWrTo
}

func (e *Exchanger) Deliver(m *Message) error {
	if m == nil {
		return nil
	}

	wires := make([]*Wire, 0)
	for _, to := range m.to {
		if w, ok := e.wires[to]; !ok {
			return errors.New("Wire not found")
		} else {
			wires = append(wires, w)
		}
	}

	ch := make(chan bool, len(wires))
	for _, w := range wires {
		go func(ch chan<- bool, w *Wire) {
			to := w.writeTo
			if to <= 0 {
				to = 1000 // 1000ms
			}
			mytimer := time.NewTimer(to * time.Millisecond)
			defer mytimer.Stop()
			select {
			case w.pipe <- m:
				ch <- true
			case <-mytimer.C:
				ch <- false
			}
		}(ch, w)
	}

	rv := true
	for i := 0; i < len(wires); i++ {
		rv = rv && <-ch
	}

	if !rv {
		return errors.New("Deliver msg to some wire timed out")
	}

	return nil
}

func (e *Exchanger) Wait(n string) *Message {
	w, ok := e.wires[n]
	if !ok {
		return nil
	}

	mytimer := time.NewTimer(w.readTo * time.Millisecond)
	defer mytimer.Stop()

	select {
	case msg := <-w.pipe:
		return msg
	case <-mytimer.C:
		return nil
	}
}
