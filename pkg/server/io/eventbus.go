package io

import (
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/server/metrics"
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
	// OriginSession is the session_id of the client whose mutation produced
	// this event. fanout skips the subscriber bound to the same session: that
	// client already updated its own cache optimistically when it made the
	// change (Create primes, Write bumps, Unlink/Rmdir invalidate), so echoing
	// the event back only destroys those primes and forces a needless GetAttr
	// refetch — the metadata-storm that made npm-install-over-WAN crawl. Empty
	// for heartbeats and pre-session events, which broadcast to every subscriber.
	OriginSession string
}

// EventBus is the server-side change-event broker. Sub-spec D's
// mutating handlers call Emit; SubscribeController drains via
// Subscribe. The interface lets us swap in inotify-driven sources
// later (roadmap future-work).
type EventBus interface {
	Emit(volume, path string, newVersion uint64, kind EventKind, originSession string)
	EmitRename(volume, oldPath, newPath string, newVersion uint64, originSession string)
	// HasSubscribers reports whether volume currently has at least one
	// subscriber. Mutating handlers consult it to skip the version-seeding
	// stat and the emit entirely when nobody is listening — see the PERF
	// note in pkg/server/controller/emit.go for the (deliberate) race
	// semantics of that gate.
	HasSubscribers(volume string) bool
	// Subscribe registers a subscriber for volume. It returns the event
	// channel, a done channel that is closed when the subscription ends
	// (cancel, bus Close, or a buffer-overflow drop), and a cancel func.
	// The event channel is NEVER closed — consumers MUST select on done to
	// detect termination (closing ch would race a concurrent fanout send and
	// panic; SS-M1). Drain contract: read ch until done fires.
	//
	// sessionID identifies the subscriber's client session so fanout can skip
	// echoing that client's own mutations back to it (see Event.OriginSession).
	// Empty (e.g. the server could not read session-id metadata) disables the
	// skip for this subscriber — it then receives every event including its
	// own, the pre-suppression behaviour.
	Subscribe(volume, sessionID string) (events <-chan Event, done <-chan struct{}, cancel func())
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
	// OnSubscribe, when non-nil, is invoked synchronously after a subscriber
	// has been registered for volume (outside the bus lock). Test seam: lets
	// tests wait deterministically for a Subscribe stream's registration
	// instead of sleeping. Unused in production.
	OnSubscribe func(volume string)
}

type subscriber struct {
	ch chan Event
	// done is closed exactly once (via closeOnce) when the subscription ends.
	// The event channel ch is NEVER closed: closing it would race a concurrent
	// fanout send and panic (SS-M1). trySend selects on done so a send in
	// flight observes termination without the closer touching ch.
	done      chan struct{}
	closeOnce sync.Once
	// sessionID is the subscribing client's session. fanout skips this
	// subscriber for events whose OriginSession matches (self-echo). Empty
	// disables the skip (receives everything).
	sessionID string
}

// terminate signals end-of-subscription. Idempotent and never closes ch, so
// it is safe to call concurrently with a fanout send (SS-M1).
func (s *subscriber) terminate() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *subscriber) trySend(ev Event) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case <-s.done:
		return false
	case s.ch <- ev:
		return true
	default:
		// Buffer full: this subscriber is too slow. Terminate it (signal done,
		// never close ch) and report the drop so the caller can splice it out
		// of the volume's subscriber slice (SS-L1).
		s.terminate()
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

func (b *localEventBus) Emit(volume, path string, newVersion uint64, kind EventKind, originSession string) {
	b.fanout(volume, Event{Path: path, NewVersion: newVersion, Kind: kind, OriginSession: originSession})
}

func (b *localEventBus) EmitRename(volume, oldPath, newPath string, newVersion uint64, originSession string) {
	b.fanout(volume, Event{Path: oldPath, NewPath: newPath, NewVersion: newVersion, Kind: KindRenamed, OriginSession: originSession})
}

func (b *localEventBus) HasSubscribers(volume string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[volume]) > 0
}

func (b *localEventBus) Subscribe(volume, sessionID string) (<-chan Event, <-chan struct{}, func()) {
	b.mu.Lock()
	if b.closed {
		// Return an already-terminated subscriber so the caller drains cleanly.
		b.mu.Unlock()
		done := make(chan struct{})
		close(done)
		return make(chan Event), done, func() {}
	}
	s := &subscriber{ch: make(chan Event, b.opts.BufferSize), done: make(chan struct{}), sessionID: sessionID}
	b.subscribers[volume] = append(b.subscribers[volume], s)
	b.mu.Unlock()
	if b.opts.OnSubscribe != nil {
		b.opts.OnSubscribe(volume)
	}
	cancel := func() {
		b.mu.Lock()
		b.removeLocked(volume, s)
		b.mu.Unlock()
		s.terminate()
	}
	return s.ch, s.done, cancel
}

// removeLocked splices s out of volume's subscriber slice. Caller holds b.mu.
// A no-op when s is already gone (e.g. an overflow-drop removed it first).
func (b *localEventBus) removeLocked(volume string, s *subscriber) {
	subs := b.subscribers[volume]
	for i, x := range subs {
		if x == s {
			b.subscribers[volume] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

func (b *localEventBus) Close() {
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, subs := range b.subscribers {
		for _, s := range subs {
			s.terminate()
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
	var dropped []*subscriber
	for _, s := range subs {
		// Self-echo skip: never deliver a client its own mutation. The client
		// already updated its cache optimistically when it made the change;
		// re-delivering the event would evict those primes and force a GetAttr
		// refetch. Guarded on a non-empty OriginSession so heartbeats and
		// pre-session events (OriginSession=="") still reach everyone, and on a
		// non-empty subscriber session so a sessionless subscriber is unaffected.
		if ev.OriginSession != "" && s.sessionID == ev.OriginSession {
			continue
		}
		if !s.trySend(ev) {
			// trySend's overflow path already terminated s. Collect it so we
			// can splice it out of the live slice — otherwise the dead
			// subscriber is re-iterated and re-counted on every future fanout
			// (SS-L1). A send that fails because s was already cancelled is
			// harmless to re-remove (removeLocked is a no-op then).
			dropped = append(dropped, s)
			if b.opts.Metrics != nil {
				b.opts.Metrics.SubscribeDroppedSlowInc(volume)
			}
		}
	}
	if len(dropped) > 0 {
		b.mu.Lock()
		for _, s := range dropped {
			b.removeLocked(volume, s)
		}
		b.mu.Unlock()
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
