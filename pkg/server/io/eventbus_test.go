package io_test

import (
	"sync"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

type EventBusSuite struct{ suite.Suite }

func (s *EventBusSuite) TestEmitDeliversToSubscriber() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	events, _, cancel := bus.Subscribe("vol1")
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
	vol1Events, _, cancel1 := bus.Subscribe("vol1")
	defer cancel1()
	vol2Events, _, cancel2 := bus.Subscribe("vol2")
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
	a, _, cancelA := bus.Subscribe("v")
	defer cancelA()
	b, _, cancelB := bus.Subscribe("v")
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
	_, done, cancel := bus.Subscribe("v")
	defer cancel()

	// Don't drain — overrun the 1-deep buffer so the slow subscriber is dropped.
	bus.Emit("v", "p1", 1, io.KindMutated)
	bus.Emit("v", "p2", 2, io.KindMutated)
	bus.Emit("v", "p3", 3, io.KindMutated)

	// Termination is signalled via done (the event channel is never closed).
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("subscriber's done channel never fired after buffer overrun")
	}
}

// TestDroppedSubscriberRemovedFromSlice covers SS-L1: a slow subscriber that
// overflows must be spliced out of the volume's subscriber slice after the
// drop, so it is NOT re-counted on every subsequent fanout. With a fresh
// (drained) subscriber present alongside it, the dropped-slow counter must
// increment exactly once across two more emits.
func (s *EventBusSuite) TestDroppedSubscriberRemovedFromSlice() {
	m := metrics.NewMetrics()
	drops := func() int {
		return int(testutil.ToFloat64(m.SubscribeDroppedSlow.WithLabelValues("v")))
	}
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 1, Metrics: m})
	defer bus.Close()

	_, slowDone, cancelSlow := bus.Subscribe("v")
	defer cancelSlow()

	// Overrun the slow subscriber (buffer 1, three emits, no drain).
	bus.Emit("v", "p1", 1, io.KindMutated)
	bus.Emit("v", "p2", 2, io.KindMutated)
	bus.Emit("v", "p3", 3, io.KindMutated)

	select {
	case <-slowDone:
	case <-time.After(time.Second):
		s.FailNow("slow subscriber was not dropped")
	}
	dropsAfterFirst := drops()
	s.Require().GreaterOrEqual(dropsAfterFirst, 1, "the overrun must have recorded at least one drop")

	// Two more emits: the dropped subscriber must be gone from the slice, so no
	// further drops are recorded for it.
	bus.Emit("v", "p4", 4, io.KindMutated)
	bus.Emit("v", "p5", 5, io.KindMutated)
	s.Equal(dropsAfterFirst, drops(),
		"a dropped subscriber must be removed from the slice, not re-counted on later fanouts")
}

func (s *EventBusSuite) TestHeartbeatFires() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16, HeartbeatInterval: 30 * time.Millisecond})
	defer bus.Close()
	events, _, cancel := bus.Subscribe("v")
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

// TestHasSubscribers pins the gate the controller's emit helpers use to skip
// the post-mutation stat + emit: false with nobody listening, true while a
// subscriber is registered (per volume), false again after unsubscribe.
func (s *EventBusSuite) TestHasSubscribers() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 4})
	defer bus.Close()

	s.False(bus.HasSubscribers("v"), "fresh bus must report no subscribers")

	_, _, cancel := bus.Subscribe("v")
	s.True(bus.HasSubscribers("v"), "one subscriber must flip the gate")
	s.False(bus.HasSubscribers("other"), "the gate is per-volume")

	cancel()
	s.False(bus.HasSubscribers("v"), "unsubscribe must clear the gate")
}

// TestHasSubscribersAfterClose: Close nils the subscriber map; the gate must
// keep answering false, not panic.
func (s *EventBusSuite) TestHasSubscribersAfterClose() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 4})
	_, _, cancel := bus.Subscribe("v")
	defer cancel()
	bus.Close()
	s.False(bus.HasSubscribers("v"))
}

// TestSubscribeAfterClose verifies that a Subscribe call after Close returns a
// pre-terminated subscription (done already closed) instead of panicking or
// writing to a nil map. The event channel is never closed — done is the signal.
func (s *EventBusSuite) TestSubscribeAfterClose() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 16})
	bus.Close()

	_, done, cancel := bus.Subscribe("vol")
	defer cancel()

	// done must already be closed — reading from it unblocks immediately.
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Subscribe-after-Close returned a subscription whose done never fired")
	}
}

// TestEmitWhileCancelClose_NoPanic covers SS-M1: under -race, spamming Emit
// while subscribers concurrently cancel and the bus is concurrently Closed must
// never panic with "send on closed channel". The pre-fix trySend closed the
// event channel from the overflow/close paths while a fanout send was in
// flight — a reachable TOCTOU panic.
func (s *EventBusSuite) TestEmitWhileCancelClose_NoPanic() {
	bus := io.NewLocalEventBus(io.EventBusOptions{BufferSize: 1})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Churn subscribers: subscribe, briefly drain, cancel — repeatedly.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ev, done, cancel := bus.Subscribe("v")
				// Drain a few then drop without fully draining (forces overflow).
				for i := 0; i < 3; i++ {
					select {
					case <-ev:
					case <-done:
					default:
					}
				}
				cancel()
			}
		}()
	}

	// Spam emits concurrently.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					bus.Emit("v", "p", 1, io.KindMutated)
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond)
	bus.Close() // concurrent Close while emits/cancels are in flight
	close(stop)
	wg.Wait()
}

func TestEventBusSuite(t *testing.T) { suite.Run(t, new(EventBusSuite)) }
