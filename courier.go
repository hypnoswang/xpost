package xpost

import (
//	"log"
)

// Master implement a Base class of Courier
// All the user defined Couriers should have Master as an anonymous member
type Master struct {
	id      int
	wirecap int
	sender  bool // a sender only produce message and never receive message from the wire
	name    string
	xp      *Xpost
}

// Courier represent a participant in the Xpost world
type Courier interface {
	GetName() string
	GetWireCap() int
	IsSender() bool
	GetID() int
	GetXpost() *Xpost

	SetName(n string)
	SetWireCap(n int)
	SetSender(b bool)
	setID(n int)
	setXpost(xp *Xpost)

	Wait() *Message
	Process(msg *Message) *Message
	Post(msg *Message) *Message

	Start() bool
	Stop()
}

// CourierCreator represents a Courier factory interface
// Xpost use this interface to create multiple instances of a courier
type CourierCreator interface {
	Create() Courier
}

type courierJob struct {
	courier Courier
}

// Run runs the courier's Wait(), Process(), Post() sequentially
func (c *courierJob) Run() {
	run(c.courier)
}

// GetName returns the courier's name
func (m Master) GetName() string {
	return m.name
}

// GetWireCap returns the capacity of the courier's wire
func (m Master) GetWireCap() int {
	return m.wirecap
}

// GetID returns the courier's ID
func (m Master) GetID() int {
	return m.id
}

// IsSender returns if the courier is a Sender
func (m Master) IsSender() bool {
	return m.sender
}

// GetXpost returns which Xpost this courier has been registered to
func (m Master) GetXpost() *Xpost {
	return m.xp
}

// SetName set the courier'name
func (m *Master) SetName(n string) {
	m.name = n
}

// SetWireCap set the capacity of the courier's wire
func (m *Master) SetWireCap(n int) {
	m.wirecap = n
}

// SetSender set if this courier is a sender
func (m *Master) SetSender(b bool) {
	m.sender = b
}

func (m *Master) setID(n int) {
	m.id = n
}

func (m *Master) setXpost(xp *Xpost) {
	m.xp = xp
}

// Wait is the entrance of the courier, it gets a message from its wire for later process
// or generate a message in the user defined Wait func
func (m *Master) Wait() *Message {
	msg := m.xp.ex.wait(m.name)

	return msg
}

// Process handle the message
func (m *Master) Process(msg *Message) *Message {
	return msg
}

// Post is the exit of the courier, it whether deliver the message to some wires or deliver
// to somewhere else in the user define Post func
func (m *Master) Post(msg *Message) *Message {
	err := m.xp.ex.deliver(msg)
	if err != nil {
		return nil
	}

	return msg
}

// Start git an opportunity that user can do some bootstrap work for the courier
// for example, an kafka consummer courier may want to start some goroutines during
// boostrap
func (m *Master) Start() bool {
	return true
}

// Stop git an opportunity that user can do some cleaning work for the courier
// for example, an kafka consummer courier may want to notify the underlying
// goroutines to exit
func (m *Master) Stop() {
}

func run(courier Courier) {
	defer func() {
		if e := recover(); e != nil {
			logErrorln(e)
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
