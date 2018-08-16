package xpost

import (
	//"log"
	"time"
)

type Master struct {
	id      int
	wirecap int
	waitto  time.Duration
	postto  time.Duration
	procto  time.Duration
	name    string
	xp      *Xpost
}

type Courier interface {
	GetName() string
	GetWireCap() int
	GetWaitTimeout() time.Duration
	GetPostTimeout() time.Duration
	GetProcessTimeout() time.Duration
	GetId() int
	getXpost() *Xpost

	SetName(n string)
	SetWireCap(n int)
	SetPostTimeout(to time.Duration)
	SetWaitTimeout(to time.Duration)
	SetProcessTimeout(to time.Duration)
	setId(n int)
	setXpost(xp *Xpost)

	Wait() *Message
	Process(msg *Message) *Message
	Post(msg *Message) *Message

	Start() bool
	Stop()
}

type CourierCreator func() Courier

func (m Master) GetName() string {
	return m.name
}

func (m Master) GetWireCap() int {
	return m.wirecap
}

func (m Master) GetWaitTimeout() time.Duration {
	return m.waitto
}

func (m Master) GetPostTimeout() time.Duration {
	return m.postto
}

func (m Master) GetProcessTimeout() time.Duration {
	return m.procto
}

func (m Master) GetId() int {
	return m.id
}

func (m Master) getXpost() *Xpost {
	return m.xp
}

func (m *Master) SetName(n string) {
	m.name = n
}

func (m *Master) SetWireCap(n int) {
	m.wirecap = n
}

func (m *Master) SetWaitTimeout(to time.Duration) {
	m.waitto = to
}

func (m *Master) SetPostTimeout(to time.Duration) {
	m.postto = to
}

func (m *Master) SetProcessTimeout(to time.Duration) {
	m.procto = to
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
	job1 := NewMsgJob(WaitT, courier, nil)
	if nil == job1 {
		return
	}

	msg1 := courier.getXpost().mpools[WaitT].Dispatch(job1)
	if nil == msg1 {
		return
	}

	job2 := NewMsgJob(ProcessT, courier, msg1)
	if nil == job2 {
		return
	}
	msg2 := courier.getXpost().mpools[ProcessT].Dispatch(job2)
	if nil == msg2 {
		return
	}

	job3 := NewMsgJob(PostT, courier, msg2)
	if nil == job3 {
		return
	}
	msg3 := courier.getXpost().mpools[PostT].Dispatch(job3)
	if nil == msg3 {
		return
	}
}
