// Package expresslane provides an in memory work queue that uses channels for
// communication between workers and the main thread when needed.
//
// The name comes from the the express lane in your local grocery store and not
// the one in the freeway. And much like the express lane at your grocery
// store, it's meant for just a few items at a time.
package expresslane

import (
	"sync"
	"time"
)

// Worker is a function that is registered to handle certain work items.
type Worker func(Item) Ack

// Queue represents a stand along queue. Alter the Tick value to modify the
// interval in which new work is checked for.
type Queue struct {
	mux     *sync.Mutex
	buf     []*Item
	lineup  map[string]*sync.Mutex
	workers map[string][]Worker
	active  bool
	ticker  *time.Ticker
	Tick    time.Duration
}

// Ack is a response from a worker. It includes the return value and an error.
type Ack struct {
	Data interface{}
	Err  error
}

// Item is an item of work sent to a worker.
type Item struct {
	Topic string
	Data  interface{}
	ch    chan []Ack
}

// New creates a new queue that is not started yet.
func New() *Queue {
	return &Queue{
		mux:     &sync.Mutex{},
		workers: make(map[string][]Worker),
		lineup:  make(map[string]*sync.Mutex),
		Tick:    time.Millisecond * 100,
	}
}

// Push queues work to be done by a worker and returns a channel used to
// communicate back to the main thread when the work is complete.
func (q *Queue) Push(topic string, data interface{}) chan []Ack {
	item := Item{Topic: topic, Data: data}
	q.mux.Lock()
	defer q.mux.Unlock()
	q.buf = append(q.buf, &item)
	item.ch = make(chan []Ack, 1)
	return item.ch
}

// Register assigns a worker to a topic
func (q *Queue) Register(topic string, worker Worker) *Queue {
	q.mux.Lock()
	defer q.mux.Unlock()

	_, ok := q.workers[topic]
	if !ok {
		q.workers[topic] = []Worker{}
	}

	q.workers[topic] = append(q.workers[topic], worker)
	return q
}

// Start makes the queue available for work. Work items are check for in a
// timer.
func (q *Queue) Start() *Queue {
	q.mux.Lock()

	if q.active {
		panic("queue is already active")
	}

	q.active = true
	q.mux.Unlock()
	q.ticker = time.NewTicker(q.Tick)

	go func() {
		for range q.ticker.C {
			if len(q.buf) > 0 && q.active {
				q.work()
			}
		}
	}()

	return q
}

// Stop prevents a queue from checking for more work until Start is called
// again.
func (q *Queue) Stop() {
	q.mux.Lock()
	defer q.mux.Unlock()
	q.active = false

	if q.ticker != nil {
		q.ticker.Stop()
	}
}

func (q *Queue) work() {
	q.mux.Lock()
	item, rest := q.buf[0], q.buf[1:]
	q.buf = rest
	go q.do(item)
	q.mux.Unlock()
}

func (q *Queue) do(item *Item) {
	var wg sync.WaitGroup
	var acks []Ack

	mux, ok := q.lineup[item.Topic]
	if !ok {
		mux = &sync.Mutex{}
		q.lineup[item.Topic] = mux
	}

	mux.Lock()

	if workers, ok := q.workers[item.Topic]; ok {
		for _, worker := range workers {
			wg.Add(1)
			go func(worker Worker) {
				defer wg.Done()
				acks = append(acks, worker(*item))
			}(worker)
		}
	}

	wg.Wait()
	item.ch <- acks
	mux.Unlock()
}
