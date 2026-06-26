package eventstore_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yourusername/collab-docs/internal/domain"
	"github.com/yourusername/collab-docs/internal/eventstore"
)

// newStore is a test helper that creates a store wired to the default logger.
func newStore(t *testing.T) *eventstore.InMemoryStore {
	t.Helper()
	return eventstore.NewInMemoryStore(slog.Default())
}

// makeEvent builds a minimal valid event for docID.
func makeEvent(docID, userID string) domain.Event {
	return domain.Event{
		ID:         uuid.NewString(),
		DocumentID: docID,
		Type:       domain.EventTypeTextInserted,
		UserID:     userID,
		OccurredAt: time.Now(),
		Payload:    []byte(`{"position":0,"text":"a"}`),
	}
}

func Test_InMemoryStore_Append_AssignsSequentialSequenceNumbers(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	const doc1, doc2 = "doc-seq-1", "doc-seq-2"

	for i := 0; i < 5; i++ {
		e, err := store.Append(ctx, makeEvent(doc1, "u1"))
		if err != nil {
			t.Fatalf("doc1 append %d: %v", i, err)
		}
		if want := int64(i + 1); e.SequenceNumber != want {
			t.Errorf("doc1[%d]: want seq %d, got %d", i, want, e.SequenceNumber)
		}
	}
	for i := 0; i < 5; i++ {
		e, err := store.Append(ctx, makeEvent(doc2, "u2"))
		if err != nil {
			t.Fatalf("doc2 append %d: %v", i, err)
		}
		if want := int64(i + 1); e.SequenceNumber != want {
			t.Errorf("doc2[%d]: want seq %d, got %d", i, want, e.SequenceNumber)
		}
	}
}

func Test_InMemoryStore_Load_ReturnsEventsInOrder(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	docID := "doc-load-order"

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := store.Append(ctx, makeEvent(docID, "u1")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	events, err := store.Load(ctx, docID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := len(events); got != n {
		t.Fatalf("want %d events, got %d", n, got)
	}
	for i, e := range events {
		if want := int64(i + 1); e.SequenceNumber != want {
			t.Errorf("events[%d]: want seq %d, got %d", i, want, e.SequenceNumber)
		}
	}
}

func Test_InMemoryStore_LoadFrom_ReturnsOnlyEventsAfterSequence(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	docID := "doc-loadfrom"

	for i := 0; i < 10; i++ {
		if _, err := store.Append(ctx, makeEvent(docID, "u1")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	events, err := store.LoadFrom(ctx, docID, 5)
	if err != nil {
		t.Fatalf("loadfrom: %v", err)
	}
	if want := 5; len(events) != want {
		t.Fatalf("want %d events after seq 5, got %d", want, len(events))
	}
	for _, e := range events {
		if e.SequenceNumber <= 5 {
			t.Errorf("got event with seq %d (want > 5)", e.SequenceNumber)
		}
	}
}

func Test_InMemoryStore_Load_UnknownDocument_ReturnsEmptySlice(t *testing.T) {
	store := newStore(t)
	events, err := store.Load(context.Background(), "nonexistent-doc")
	if err != nil {
		t.Fatalf("want nil error for unknown doc, got: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("want empty slice, got %d events", len(events))
	}
}

func Test_InMemoryStore_Subscribe_ReceivesNewEventsAfterSubscription(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	docID := "doc-subscribe"

	ch, unsub := store.Subscribe(docID)
	defer unsub()

	evt := makeEvent(docID, "u1")
	saved, err := store.Append(ctx, evt)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	select {
	case received := <-ch:
		if received.ID != saved.ID {
			t.Errorf("received wrong event: want ID %s, got %s", saved.ID, received.ID)
		}
		if received.SequenceNumber != saved.SequenceNumber {
			t.Errorf("sequence mismatch: want %d, got %d", saved.SequenceNumber, received.SequenceNumber)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: expected subscribed event within 2s")
	}
}

func Test_InMemoryStore_Unsubscribe_StopsDelivery(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	docID := "doc-unsub"

	ch, unsub := store.Subscribe(docID)
	unsub()

	if _, err := store.Append(ctx, makeEvent(docID, "u1")); err != nil {
		t.Fatalf("append: %v", err)
	}

	select {
	case <-ch:
		// A single buffered event arriving before unsubscribe completed is acceptable.
		// What matters is no further events arrive.
		select {
		case <-ch:
			t.Error("received a second event after unsubscribe")
		case <-time.After(100 * time.Millisecond):
		}
	case <-time.After(100 * time.Millisecond):
		// Good: nothing arrived after unsubscribe.
	}
}

func Test_InMemoryStore_ConcurrentAppends_NoDuplicateSequenceNumbers(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	docID := "doc-concurrent"

	const goroutines = 50
	const perGoroutine = 20
	const total = goroutines * perGoroutine

	results := make(chan domain.Event, total)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				e, err := store.Append(ctx, makeEvent(docID, "u"))
				if err != nil {
					t.Errorf("concurrent append: %v", err)
					return
				}
				results <- e
			}
		}()
	}

	wg.Wait()
	close(results)

	seen := make(map[int64]struct{}, total)
	for e := range results {
		if _, dup := seen[e.SequenceNumber]; dup {
			t.Errorf("duplicate sequence number: %d", e.SequenceNumber)
		}
		seen[e.SequenceNumber] = struct{}{}
	}
	if len(seen) != total {
		t.Errorf("want %d unique sequences, got %d", total, len(seen))
	}
}
