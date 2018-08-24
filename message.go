package xpost

import (
	"sync"
)

// Message is the basic communication unit in Xpost world
type Message struct {
	from string
	dest []string
	body []byte
}

var msgPool sync.Pool

func init() {
	msgPool = sync.Pool{
		New: func() interface{} {
			return newMessage()
		},
	}
}

// GetMessage get a new and fresh message instance from the pool
// using GetMessage() and m.Free() instead of NewMessage is of more efficiency
func GetMessage() *Message {
	msg := msgPool.Get().(*Message)
	msg.reset()

	return msg
}

// Free put the message instance back to the pool
func (m *Message) Free() {
	msgPool.Put(m)
}

func newMessage() *Message {
	return &Message{}
}

// SetFrom set the message's sender
func (m *Message) SetFrom(from string) {
	m.from = from
}

// AppendDest append new destinations to the message
func (m *Message) AppendDest(dest ...string) {
	m.dest = append(m.dest, dest...)
}

// SetDest asign destinations to the message
func (m *Message) SetDest(dest ...string) {
	m.dest = dest
}

// SetBody the content of the message
func (m *Message) SetBody(body []byte) {
	m.body = body
}

func (m *Message) reset() {
	m.from = ""
	m.dest = make([]string, 0)
	m.body = make([]byte, 0)
}

// GetFrom returns the message's sender
func (m Message) GetFrom() string {
	return m.from
}

// GetDest returns the destinations of the message
func (m Message) GetDest() []string {
	return m.dest
}

// GetBody returns the content of the message
func (m Message) GetBody() []byte {
	return m.body
}
