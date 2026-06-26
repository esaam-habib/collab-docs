// Package eventstore provides an append-only event log for the event sourcing pattern.
package eventstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/yourusername/collab-docs/internal/domain"
)

// subscriberBufferSize is the number of events that can be buffered per subscriber
// before the store starts dropping events for that subscriber.
const subscriberBufferSize = 64

// EventStore is an append-only, ordered log of domain events.
// Implementations must be safe for concurrent use.
//
// Justification for > 3 methods: the interface models a complete event store
// abstraction (write, read-all, read-from, and observe) that cannot be split
// without creating a leaky abstraction. Each caller uses a subset.
type EventStore interface {
	// Append atomically appends an event and assigns its SequenceNumber.
	// The returned event is the stored copy with SequenceNumber set.
	Append(ctx context.Context, event domain.Event) (domain.Event, error)

	// Load returns all events for documentID in ascending sequence order.
	// Returns an empty slice (not an error) when no events exist.
	Load(ctx context.Context, documentID string) ([]domain.Event, error)

	// LoadFrom returns events for documentID with SequenceNumber > afterSequence,
	// in ascending order.
	LoadFrom(ctx context.Context, documentID string, afterSequence int64) ([]domain.Event, error)

	// Subscribe registers a channel to receive events appended after the call.
	// The returned unsubscribe function must be called exactly once when done.
	Subscribe(documentID string) (<-chan domain.Event, func())
}

// subscriber holds a single subscriber's delivery channel.
type subscriber struct {
	ch chan domain.Event
}

// InMemoryStore is a thread-safe, in-memory implementation of EventStore.
// It is suitable for development and testing; replace with a durable backend for production.
type InMemoryStore struct {
	logger *slog.Logger

	// eventMu protects the events and sequences maps.
	// Held for write only during Append; read for Load/LoadFrom.
	eventMu   sync.RWMutex
	events    map[string][]domain.Event // documentID → ordered events
	sequences map[string]int64          // documentID → last assigned sequence number

	// subMu protects the subscribers map independently of eventMu
	// to allow subscriber operations to proceed concurrently with reads.
	subMu       sync.Mutex
	subscribers map[string][]*subscriber // documentID → registered subscribers
}

// NewInMemoryStore returns a ready-to-use InMemoryStore backed by the given logger.
func NewInMemoryStore(logger *slog.Logger) *InMemoryStore {
	return &InMemoryStore{
		logger:      logger,
		events:      make(map[string][]domain.Event),
		sequences:   make(map[string]int64),
		subscribers: make(map[string][]*subscriber),
	}
}

// Append atomically assigns a SequenceNumber and persists the event.
// Subscribers receive the event asynchronously in separate goroutines.
func (s *InMemoryStore) Append(ctx context.Context, event domain.Event) (domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return domain.Event{}, fmt.Errorf("eventstore: append: %w", err)
	}

	// Critical section: assign sequence number and persist.
	s.eventMu.Lock()
	s.sequences[event.DocumentID]++
	event.SequenceNumber = s.sequences[event.DocumentID]
	s.events[event.DocumentID] = append(s.events[event.DocumentID], event)
	s.eventMu.Unlock()

	// Snapshot subscribers outside the write lock to minimise contention.
	s.subMu.Lock()
	subs := make([]*subscriber, len(s.subscribers[event.DocumentID]))
	copy(subs, s.subscribers[event.DocumentID])
	s.subMu.Unlock()

	// Deliver to each subscriber in its own goroutine.
	// Owner: each goroutine terminates after a single non-blocking send attempt.
	for _, sub := range subs {
		sub := sub
		go func() {
			select {
			case sub.ch <- event:
			default:
				s.logger.Warn("eventstore: subscriber channel full, dropping event",
					slog.String("documentID", event.DocumentID),
					slog.Int64("sequence", event.SequenceNumber),
				)
			}
		}()
	}

	return event, nil
}

// Load returns all stored events for documentID in ascending sequence order.
// Returns an empty (non-nil) slice if no events exist.
func (s *InMemoryStore) Load(ctx context.Context, documentID string) ([]domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: load: %w", err)
	}
	s.eventMu.RLock()
	raw := s.events[documentID]
	out := make([]domain.Event, len(raw))
	copy(out, raw)
	s.eventMu.RUnlock()
	return out, nil
}

// LoadFrom returns events for documentID with SequenceNumber strictly greater than afterSequence.
func (s *InMemoryStore) LoadFrom(ctx context.Context, documentID string, afterSequence int64) ([]domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: loadfrom: %w", err)
	}
	s.eventMu.RLock()
	raw := s.events[documentID]
	var out []domain.Event
	for _, e := range raw {
		if e.SequenceNumber > afterSequence {
			out = append(out, e)
		}
	}
	s.eventMu.RUnlock()
	return out, nil
}

// Subscribe registers a buffered channel that receives events appended after this call.
// The caller must call the returned unsubscribe function exactly once when done.
func (s *InMemoryStore) Subscribe(documentID string) (<-chan domain.Event, func()) {
	sub := &subscriber{
		ch: make(chan domain.Event, subscriberBufferSize),
	}

	s.subMu.Lock()
	s.subscribers[documentID] = append(s.subscribers[documentID], sub)
	s.subMu.Unlock()

	unsubscribe := func() {
		s.subMu.Lock()
		defer s.subMu.Unlock()
		subs := s.subscribers[documentID]
		for i, candidate := range subs {
			if candidate == sub {
				s.subscribers[documentID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		// Drain channel so any in-flight goroutine senders can unblock.
		for len(sub.ch) > 0 {
			<-sub.ch
		}
	}

	return sub.ch, unsubscribe
}
