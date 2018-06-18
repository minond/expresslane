// +build ignore
package main

import (
	"log"
	"runtime"
	"time"

	el "github.com/minond/expresslane"
)

func main() {
	log.Println("this example will continue to run forever")
	time.Sleep(time.Second)
	q := el.New()

	q.Register("seconds", func(item el.Item) el.Ack {
		log.Printf("topic '%s', data: %v\n", item.Topic, item.Data)
		return el.Ack{}
	})

	q.Register("milliseconds", func(item el.Item) el.Ack {
		log.Printf("topic '%s', data: %v\n", item.Topic, item.Data)
		return el.Ack{}
	})

	go func() {
		timer := time.NewTicker(time.Millisecond * 100)
		for x := range timer.C {
			q.Push(el.Item{Topic: "milliseconds", Data: x})
		}
	}()

	go func() {
		timer := time.NewTicker(time.Second)
		for x := range timer.C {
			q.Push(el.Item{Topic: "seconds", Data: x})
		}
	}()

	go q.Run()
	runtime.Goexit()
}