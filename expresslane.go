package expresslane

import (
	"sync"
	"time"
)

type Worker func(Item) error

type Queue struct {
	mux     sync.Mutex
	buf     []Item
	workers map[string][]Worker
	active  bool
	ticker  *time.Ticker
	Tick    time.Duration
}

type Item struct {
	Topic string
	Data  interface{}
}

func New() *Queue {
	return &Queue{
		workers: make(map[string][]Worker),
		Tick:    time.Millisecond * 100,
	}
}

func (q *Queue) Push(item Item) *Queue {
	q.mux.Lock()
	defer q.mux.Unlock()
	q.buf = append(q.buf, item)
	return q
}

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

func (q *Queue) Run() *Queue {
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
				q.AssumeWork()
			}
		}
	}()

	return q
}

func (q *Queue) Stop() {
	q.mux.Lock()
	defer q.mux.Unlock()
	q.ticker.Stop()
	q.active = false
}

func (q *Queue) AssumeWork() {
	q.mux.Lock()
	item, rest := q.buf[0], q.buf[1:]
	q.buf = rest
	q.Do(item)
	q.mux.Unlock()
}

func (q *Queue) Do(item Item) {
	if workers, ok := q.workers[item.Topic]; ok {
		for _, worker := range workers {
			go worker(item)
		}
	}
}
