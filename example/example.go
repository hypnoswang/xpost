package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"xpost"
)

type MsgGenerator struct {
	xpost.Master
}

func NewMsgGenerator() *MsgGenerator {
	mg := &MsgGenerator{}

	mg.SetName("MsgGen")
	mg.SetWireCap(100)

	return mg
}

func (mg *MsgGenerator) Wait() *xpost.Message {
	time.Sleep(800 * time.Millisecond)
	//	log.Printf("In MsgGenerator.Wait\n")
	msg := xpost.GetMessage()
	if nil == msg {
		log.Fatal("Get new message type failed") // will call os.Exit(1)
	}

	msg.SetFrom(mg.GetName())
	msg.SetDest("MsgAgency")
	cnt := fmt.Sprintf("%s.%d: let's go Hippo!!", mg.GetName(), mg.GetId())
	msg.SetMsg([]byte(cnt))

	return msg
}

type MsgAgency struct {
	xpost.Master
}

func NewMsgAgency() *MsgAgency {
	ma := &MsgAgency{}

	ma.SetName("MsgAgency")
	ma.SetWireCap(100)

	return ma
}

func (ma *MsgAgency) Process(msg *xpost.Message) *xpost.Message {
	if msg == nil {
		return nil
	}

	//	log.Printf("In MsgAgency.Process\n")

	cnt := string(msg.GetMsg())
	newcnt := fmt.Sprintf("Agency transit msg: %s", cnt)
	msg.SetMsg([]byte(newcnt))
	msg.SetFrom(ma.GetName())
	msg.SetDest("MsgOuter")

	return msg
}

type MsgOuter struct {
	xpost.Master
}

func NewMsgOuter() *MsgOuter {
	mo := &MsgOuter{}

	mo.SetName("MsgOuter")
	mo.SetWireCap(100)

	return mo
}

func (mo *MsgOuter) Post(msg *xpost.Message) *xpost.Message {
	if msg == nil {
		return nil
	}

	cnt := string(msg.GetMsg())

	log.Printf("MsgOuter %d received msg: %s\n", mo.GetId(), cnt)

	msg.Free() // free this msg for reuse

	return msg
}

func main() {
	rv := true

	rv = rv && xpost.GetXpost().RegisterCourier(
		func() xpost.Courier {
			return NewMsgGenerator()
		}, 2)

	rv = rv && xpost.GetXpost().RegisterCourier(
		func() xpost.Courier {
			return NewMsgAgency()
		}, 1)

	rv = rv && xpost.GetXpost().RegisterCourier(
		func() xpost.Courier {
			return NewMsgOuter()
		}, 2)

	if !rv {
		log.Fatal("Register Courier failed")
	}

	xpost.GetXpost().SetMsgPoolSize(xpost.WaitT, 16)
	xpost.GetXpost().SetMsgPoolSize(xpost.ProcessT, 16)
	xpost.GetXpost().SetMsgPoolSize(xpost.PostT, 16)
	xpost.GetXpost().SetDumpInfoInterval(10000)

	if !xpost.GetXpost().Init() {
		log.Fatalln("Initialize xpost failed")
	}

	xpost.GetXpost().Run()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	xpost.GetXpost().Stop()

	log.Println("System Exit...")
}
