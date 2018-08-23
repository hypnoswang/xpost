package xpost

import (
	"log"
)

type Master struct {
	id      int
	wirecap int
	sender  bool
	name    string
	xp      *Xpost
}

type Courier interface {
	GetName() string
	GetWireCap() int
	IsSender() bool
	GetId() int
	GetXpost() *Xpost

	SetName(n string)
	SetWireCap(n int)
	SetSender(b bool)
	setId(n int)
	setXpost(xp *Xpost)

	Wait() *Message
	Process(msg *Message) *Message
	Post(msg *Message) *Message

	Start() bool
	Stop()
}

type CourierCreator interface {
	Create() Courier
}

type CourierJob struct {
	courier Courier
}

func (c *CourierJob) Run() {
	run(c.courier)
}

func (m Master) GetName() string {
	return m.name
}

func (m Master) GetWireCap() int {
	return m.wirecap
}

func (m Master) GetId() int {
	return m.id
}

func (m Master) IsSender() bool {
	return m.sender
}

func (m Master) GetXpost() *Xpost {
	return m.xp
}

func (m *Master) SetName(n string) {
	m.name = n
}

func (m *Master) SetWireCap(n int) {
	m.wirecap = n
}

func (m *Master) SetSender(b bool) {
	m.sender = b
}

func (m *Master) setId(n int) {
	m.id = n
}

func (m *Master) setXpost(xp *Xpost) {
	m.xp = xp
}

func (m *Master) Wait() *Message {
	msg := m.xp.ex.Wait(m.name)

	return msg
}

func (m *Master) Process(msg *Message) *Message {
	return msg
}

func (m *Master) Post(msg *Message) *Message {
	err := m.xp.ex.Deliver(msg)
	if err != nil {
		return nil
	}

	return msg
}

func (m *Master) Start() bool {
	return true
}

func (m *Master) Stop() {
}

func run(courier Courier) {
	defer func() {
		if e := recover(); e != nil {
			log.Println(e)
		}
	}()

	msg1 := courier.Wait()
	if nil == msg1 {
		return
	}

	msg2 := courier.Process(msg1)
	if nil == msg2 {
		return
	}

	msg3 := courier.Post(msg2)
	if nil == msg3 {
		return
	}

}
