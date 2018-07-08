package expresslane

import (
	"log"
	"runtime"
	"time"
)

func Example_async() {
	log.Println("this example will continue to run forever")
	time.Sleep(time.Second)
	q := New().Start()

	q.Register("seconds", func(item Item) Ack {
		log.Printf("topic '%s', data: %v\n", item.Topic, item.Data)
		return Ack{}
	})

	q.Register("milliseconds", func(item Item) Ack {
		log.Printf("topic '%s', data: %v\n", item.Topic, item.Data)
		return Ack{}
	})

	go func() {
		timer := time.NewTicker(time.Millisecond * 100)
		for x := range timer.C {
			q.Push("milliseconds", x)
		}
	}()

	go func() {
		timer := time.NewTicker(time.Second)
		for x := range timer.C {
			q.Push("seconds", x)
		}
	}()

	runtime.Goexit()
}
