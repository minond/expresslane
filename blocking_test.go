package expresslane

import (
	"log"
	"math/rand"
	"runtime"
	"time"
)

func Example() {
	log.Println("this example will continue to run forever")
	time.Sleep(time.Second)
	q := New().Start()

	q.Register("milliseconds", func(item Item) Ack {
		log.Println("worker is sleeping....")
		time.Sleep(time.Second)
		log.Printf("topic '%s', data: %v\n", item.Topic, item.Data)
		return Ack{Data: rand.Int()}
	})

	go func() {
		timer := time.NewTicker(time.Millisecond * 100)
	dowork:
		for x := range timer.C {
			ch1 := q.Push("milliseconds", x)
			ch2 := q.Push("milliseconds", x)

			op1, op2 := true, true

			log.Println("blocking until workers are done")

			for {
				select {
				case res1 := <-ch1:
					log.Printf("response from workers (1): %v\n", res1)
					op1 = false
				case res2 := <-ch2:
					log.Printf("response from workers (2): %v\n", res2)
					op2 = false
				default:
					if !op1 && !op2 {
						goto dowork
					}
				}
			}
		}
	}()

	runtime.Goexit()
}
