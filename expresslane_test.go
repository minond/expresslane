package expresslane

import (
	"reflect"
	"testing"
)

func eq(msg string, t *testing.T, x, y interface{}) {
	if !reflect.DeepEqual(x, y) {
		t.Fatal(msg)
	}
}

func TestAQueueCanBeStopped(t *testing.T) {
	q := New()
	q.Run()
	eq("expecting q.active to be true", t, true, q.active)
	q.Stop()
	eq("expecting q.active to be false", t, false, q.active)
}

func TestWorkIsPushedToBackOfList(t *testing.T) {
	q := New()
	q.Run()
	defer q.Stop()

	q.Push(Item{Data: 1})
	q.Push(Item{Data: 2})

	eq("expecting q.buf[0].Data to be 1", t, 1, q.buf[0].Data)
	eq("expecting q.buf[1].Data to be 2", t, 2, q.buf[1].Data)
}

func TestWorkCanBeDoneOnAnUnregisteredTopic(t *testing.T) {
	q := New()
	q.Run()
	defer q.Stop()

	ch := q.Push(Item{Topic: "new", Data: 2})
	acks := <-ch

	eq("expecting to get back 0 acks", t, 0, len(acks))
}

func TestRegisteringWorkers(t *testing.T) {
	q := New()

	q.Register("anything", func(i Item) Ack { return Ack{} })
	q.Register("anything", func(i Item) Ack { return Ack{} })
	q.Register("nothing", func(i Item) Ack { return Ack{} })

	eq(`expecting len(q.workers["anything"]) to be 2`, t, 2, len(q.workers["anything"]))
	eq(`expecting len(q.workers["nothing"]) to be 1`, t, 1, len(q.workers["nothing"]))
}
