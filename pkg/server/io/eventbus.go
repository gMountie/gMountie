package io

import (
	"sync"
	"sync/atomic"
	"time"

	"gmountie/pkg/server/metrics"
)

// EventKind classifies what changed.
type EventKind int8

const (
	KindMutated EventKind = iota
	KindDeleted
	KindRenamed
	KindHeartbeat
)

// Event is the unit delivered to subscribers.
type Event struct {
	Path       string
	NewPath    string // KindRenamed only
	NewVersion uint64 // 0 for KindDeleted / KindHeartbeat
	Kind       EventKind
}

// EventBus is the server-side change-event broker. Sub-spec D's
// mutating handlers call Emit; SubscribeController drains via
// Subscribe. The interface lets us swap in inotify-driven sources
// later (roadmap future-work).
type EventBus interface {
	Emit(volume, path string, newVersion uint64, kind EventKind)
	EmitRename(volume, oldPath, newPath string, newVersion uint64)
	Subscribe(volume string) (events <-chan Event, cancel func())
	Close()
}

// EventBusOptions configures the local impl.
type EventBusOptions struct {
	// BufferSize is the per-subscriber channel capacity. Full channel
	// triggers a drop (the channel is closed; consumer reconnects).
	BufferSize int
	// HeartbeatInterval is the per-volume heartbeat tick. Zero disables
	// heartbeats (test-only mode).
	HeartbeatInterval time.Duration
	// Metrics is optional; if non-nil, subscribe counters are bumped.
	Metrics *metrics.Metrics
}

type subscriber struct {
	ch     chan Event
	closed atomic.Bool
}

func (s *subscriber) trySend(ev Event) bool {
	if s.closed.Load() {
		return false
	}
	select {
	case s.ch <- ev:
		return true
	default:
		if s.closed.CompareAndSwap(false, true) {
			close(s.ch)
		}
		return false
	}
}

type localEventBus struct {
	opts        EventBusOptions
	mu          sync.RWMutex
	subscribers map[string][]*subscriber
	closed      bool // set under mu; guards Subscribe-after-Close
	stopCh      chan struct{}
	stopOnce    sync.Once
}

// NewLocalEventBus constructs an in-process bus. The HeartbeatInterval
// goroutine starts at construction time and stops on Close.
func NewLocalEventBus(opts EventBusOptions) EventBus {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 256
	}
	b := &localEventBus{
		opts:        opts,
		subscribers: make(map[string][]*subscriber),
		stopCh:      make(chan struct{}),
	}
	if opts.HeartbeatInterval > 0 {
		go b.heartbeatLoop()
	}
	return b
}

func (b *localEventBus) Emit(volume, path string, newVersion uint64, kind EventKind) {
	b.fanout(volume, Event{Path: path, NewVersion: newVersion, Kind: kind})
}

func (b *localEventBus) EmitRename(volume, oldPath, newPath string, newVersion uint64) {
	b.fanout(volume, Event{Path: oldPath, NewPath: newPath, NewVersion: newVersion, Kind: KindRenamed})
}

func (b *localEventBus) Subscribe(volume string) (<-chan Event, func()) {
	b.mu.Lock()
	if b.closed {
		// Return an already-closed channel so the caller can range/drain cleanly.
		b.mu.Unlock()
		ch := make(chan Event)
		close(ch)
		return ch, func() {}
	}
	s := &subscriber{ch: make(chan Event, b.opts.BufferSize)}
	b.subscribers[volume] = append(b.subscribers[volume], s)
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs := b.subscribers[volume]
		for i, x := range subs {
			if x == s {
				b.subscribers[volume] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if s.closed.CompareAndSwap(false, true) {
			close(s.ch)
		}
	}
	return s.ch, cancel
}

func (b *localEventBus) Close() {
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, subs := range b.subscribers {
		for _, s := range subs {
			if s.closed.CompareAndSwap(false, true) {
				close(s.ch)
			}
		}
	}
	b.subscribers = nil
	b.closed = true
}

// eventKindString returns the Prometheus label string for an EventKind.
func eventKindString(kind EventKind) string {
	switch kind {
	case KindMutated:
		return "mutated"
	case KindDeleted:
		return "deleted"
	case KindRenamed:
		return "renamed"
	case KindHeartbeat:
		return "heartbeat"
	default:
		return "unknown"
	}
}

func (b *localEventBus) fanout(volume string, ev Event) {
	b.mu.RLock()
	subs := append([]*subscriber(nil), b.subscribers[volume]...)
	b.mu.RUnlock()
	if b.opts.Metrics != nil && len(subs) > 0 {
		b.opts.Metrics.SubscribeEventEmittedInc(eventKindString(ev.Kind))
	}
	for _, s := range subs {
		if !s.trySend(ev) && b.opts.Metrics != nil {
			b.opts.Metrics.SubscribeDroppedSlowInc(volume)
		}
	}
}

func (b *localEventBus) heartbeatLoop() {
	t := time.NewTicker(b.opts.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-t.C:
			b.mu.RLock()
			volumes := make([]string, 0, len(b.subscribers))
			for v := range b.subscribers {
				volumes = append(volumes, v)
			}
			b.mu.RUnlock()
			for _, v := range volumes {
				b.fanout(v, Event{Kind: KindHeartbeat})
			}
		}
	}
}
