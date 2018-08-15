package xpost

import (
	"sync"
)

type Message struct {
	from string
	to   []string
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

func (m *Message) AppendDest(to ...string) {
	m.to = append(m.to, to...)
}

func (m *Message) SetDest(to ...string) {
	m.to = to
}

func (m *Message) SetMsg(msg []byte) {
	m.msg = msg
}

func (m *Message) reset() {
	m.from = ""
	m.to = make([]string, 0)
	m.msg = make([]byte, 0)
}

func (m Message) GetFrom() string {
	return m.from
}

func (m Message) GetTo() []string {
	return m.to
}

func (m Message) GetMsg() []byte {
	return m.msg
}
