package expresslane

import (
	"sync"
	"time"
)

type Worker func(Item) Ack

type Queue struct {
	mux     *sync.Mutex
	buf     []*Item
	lineup  map[string]*sync.Mutex
	workers map[string][]Worker
	active  bool
	ticker  *time.Ticker
	Tick    time.Duration
}

type Ack struct {
	Data interface{}
	Err  error
}

type Item struct {
	Topic string
	Data  interface{}
	ch    chan []Ack
}

func New() *Queue {
	return &Queue{
		mux:     &sync.Mutex{},
		workers: make(map[string][]Worker),
		lineup:  make(map[string]*sync.Mutex),
		Tick:    time.Millisecond * 100,
	}
}

func (q *Queue) Push(topic string, data interface{}) chan []Ack {
	item := Item{Topic: topic, Data: data}
	q.mux.Lock()
	defer q.mux.Unlock()
	q.buf = append(q.buf, &item)
	item.ch = make(chan []Ack, 1)
	return item.ch
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
