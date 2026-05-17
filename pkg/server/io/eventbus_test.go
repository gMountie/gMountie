package io_test

import (
	"testing"
	"time"

	"gmountie/pkg/server/io"

	"github.com/stretchr/testify/suite"
)

type EventBusSuite struct{ suite.Suite }

func (s *EventBusSuite) TestEmitDeliversToSubscriber() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	events, cancel := bus.Subscribe("vol1")
	defer cancel()

	bus.Emit("vol1", "foo/bar", 42, io.KindMutated)

	select {
	case ev := <-events:
		s.Assert().Equal("foo/bar", ev.Path)
		s.Assert().Equal(uint64(42), ev.NewVersion)
		s.Assert().Equal(io.KindMutated, ev.Kind)
	case <-time.After(time.Second):
		s.FailNow("no event received")
	}
}

func (s *EventBusSuite) TestEmitOnlyDeliversToMatchingVolume() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	vol1Events, cancel1 := bus.Subscribe("vol1")
	defer cancel1()
	vol2Events, cancel2 := bus.Subscribe("vol2")
	defer cancel2()

	bus.Emit("vol1", "p", 1, io.KindMutated)

	select {
	case <-vol1Events:
	case <-time.After(time.Second):
		s.FailNow("vol1 subscriber missed event")
	}
	select {
	case ev := <-vol2Events:
		s.FailNowf("vol2 subscriber got cross-volume event", "%+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func (s *EventBusSuite) TestMultiSubscriberFanOut() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	a, cancelA := bus.Subscribe("v")
	defer cancelA()
	b, cancelB := bus.Subscribe("v")
	defer cancelB()

	bus.Emit("v", "p", 1, io.KindMutated)

	for _, ch := range []<-chan io.Event{a, b} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			s.FailNow("subscriber missed event")
		}
	}
}

func (s *EventBusSuite) TestFullChannelDropsSubscriber() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 1})
	defer bus.Close()
	events, cancel := bus.Subscribe("v")
	defer cancel()

	bus.Emit("v", "p1", 1, io.KindMutated)
	bus.Emit("v", "p2", 2, io.KindMutated)
	bus.Emit("v", "p3", 3, io.KindMutated)

	deadline := time.After(time.Second)
	closed := false
	for !closed {
		select {
		case _, ok := <-events:
			if !ok {
				closed = true
			}
		case <-deadline:
			s.FailNow("channel never closed after buffer overrun")
		}
	}
}

func (s *EventBusSuite) TestHeartbeatFires() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16, HeartbeatInterval: 30 * time.Millisecond})
	defer bus.Close()
	events, cancel := bus.Subscribe("v")
	defer cancel()

	deadline := time.After(time.Second)
	sawHeartbeat := false
	for !sawHeartbeat {
		select {
		case ev := <-events:
			if ev.Kind == io.KindHeartbeat {
				sawHeartbeat = true
			}
		case <-deadline:
			s.FailNow("no heartbeat in 1 s")
		}
	}
}

func TestEventBusSuite(t *testing.T) { suite.Run(t, new(EventBusSuite)) }
