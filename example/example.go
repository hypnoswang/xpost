package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/hypnoswang/xpost"
)

// MsgGenerator responsible for generate new messages
type MsgGenerator struct {
	xpost.Master
}

// MsgGeneratorCreator is the MsgGenerator factory
type MsgGeneratorCreator struct{}

// Create returns a new MsgGenerator instance
func (c MsgGeneratorCreator) Create() xpost.Courier {
	return newMsgGenerator()
}

func newMsgGenerator() *MsgGenerator {
	mg := &MsgGenerator{}

	mg.SetName("MsgGen")
	mg.SetWireCap(100)
	mg.SetSender(true)

	return mg
}

// Wait generate a new message every time
func (mg *MsgGenerator) Wait() *xpost.Message {
	time.Sleep(1 * time.Millisecond)
	//	log.Printf("In MsgGenerator.Wait\n")
	msg := xpost.GetMessage()
	if nil == msg {
		log.Fatal("Get new message type failed") // will call os.Exit(1)
	}

	msg.SetFrom(mg.GetName())
	msg.SetDest("MsgAgency")
	cnt := fmt.Sprintf("%s.%d: let's go Hippo!!", mg.GetName(), mg.GetID())
	msg.SetBody([]byte(cnt))

	return msg
}

// MsgAgency transit the received messages
type MsgAgency struct {
	xpost.Master
}

// MsgAgencyCreator is a MsgAgency factory
type MsgAgencyCreator struct{}

// Create returns a MsgAgency instance
func (c MsgAgencyCreator) Create() xpost.Courier {
	return newMsgAgency()
}

func newMsgAgency() *MsgAgency {
	ma := &MsgAgency{}

	ma.SetName("MsgAgency")
	ma.SetWireCap(100)

	return ma
}

// Process add some information to the message and then deliver it
func (ma *MsgAgency) Process(msg *xpost.Message) *xpost.Message {
	if msg == nil {
		return nil
	}

	cnt := string(msg.GetBody())
	newcnt := fmt.Sprintf("Agency transit msg: %s", cnt)
	msg.SetBody([]byte(newcnt))
	msg.SetFrom(ma.GetName())
	msg.SetDest("MsgOuter")

	return msg
}

// MsgOuter show the received messages
type MsgOuter struct {
	xpost.Master
}

// MsgOuterCreator is a MsgOuter factory
type MsgOuterCreator struct{}

// Create returns a Create instance
func (c MsgOuterCreator) Create() xpost.Courier {
	return newMsgOuter()
}

func newMsgOuter() *MsgOuter {
	mo := &MsgOuter{}

	mo.SetName("MsgOuter")
	mo.SetWireCap(100)

	return mo
}

// Post just show the received message
func (mo *MsgOuter) Post(msg *xpost.Message) *xpost.Message {
	if msg == nil {
		return nil
	}

	cnt := string(msg.GetBody())

	log.Printf("MsgOuter %d received msg: %s\n", mo.GetID(), cnt)

	msg.Free() // free this msg for reuse

	return msg
}

func main() {
	rv := true

	rv = rv && xpost.GetXpost().RegisterCourier(
		MsgGeneratorCreator{}, 2)

	rv = rv && xpost.GetXpost().RegisterCourier(
		MsgAgencyCreator{}, 1)

	rv = rv && xpost.GetXpost().RegisterCourier(
		MsgOuterCreator{}, 2)

	if !rv {
		log.Fatal("Register Courier failed")
	}

	xpost.GetXpost().SetDumpInfoInterval(5000)

	xpost.GetXpost().Run()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	xpost.GetXpost().Stop()

	log.Println("System Exit...")
}
