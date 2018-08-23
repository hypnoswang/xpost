package xpost

import (
	"sync"
)

type Message struct {
	from string
	dest []string
	msg  []byte
}

var msgPool sync.Pool

func init() {
	msgPool = sync.Pool{
		New: func() interface{} {
			return NewMessage()
		},
	}
}

// using GetMessage() and m.Free() instead of NewMessage is of more efficiency
func GetMessage() *Message {
	msg := msgPool.Get().(*Message)
	msg.reset()

	return msg
}

func (m *Message) Free() {
	msgPool.Put(m)
}

func NewMessage() *Message {
	return &Message{}
}

func (m *Message) SetFrom(from string) {
	m.from = from
}

func (m *Message) AppendDest(dest ...string) {
	m.dest = append(m.dest, dest...)
}

func (m *Message) SetDest(dest ...string) {
	m.dest = dest
}

func (m *Message) SetMsg(msg []byte) {
	m.msg = msg
}

func (m *Message) reset() {
	m.from = ""
	m.dest = make([]string, 0)
	m.msg = make([]byte, 0)
}

func (m Message) GetFrom() string {
	return m.from
}

func (m Message) GetDest() []string {
	return m.dest
}

func (m Message) GetMsg() []byte {
	return m.msg
}
