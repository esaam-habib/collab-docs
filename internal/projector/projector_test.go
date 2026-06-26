package projector_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yourusername/collab-docs/internal/domain"
	"github.com/yourusername/collab-docs/internal/eventstore"
	"github.com/yourusername/collab-docs/internal/projector"
)

// fixture wires a fresh projector and store for each test.
type fixture struct {
	store *eventstore.InMemoryStore
	proj  *projector.Projector
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store := eventstore.NewInMemoryStore(slog.Default())
	return &fixture{
		store: store,
		proj:  projector.New(store, slog.Default()),
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

// seed appends a single event, failing the test on error.
func (f *fixture) seed(t *testing.T, docID string, etype domain.EventType, userID string, payload any) domain.Event {
	t.Helper()
	e := domain.Event{
		ID:         uuid.NewString(),
		DocumentID: docID,
		Type:       etype,
		UserID:     userID,
		OccurredAt: time.Now(),
		Payload:    mustMarshal(t, payload),
	}
	saved, err := f.store.Append(context.Background(), e)
	if err != nil {
		t.Fatalf("seed event: %v", err)
	}
	return saved
}

func Test_Projector_Project_EmptyStore_ReturnsDocumentNotFound(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.proj.Project(context.Background(), "ghost-doc")
	if !errors.Is(err, domain.ErrDocumentNotFound) {
		t.Errorf("want ErrDocumentNotFound, got: %v", err)
	}
}

func Test_Projector_Project_AppliesInsertEventsCorrectly(t *testing.T) {
	cases := []struct {
		name    string
		inserts []domain.TextInsertedPayload
		want    string
	}{
		{
			name:    "single insert",
			inserts: []domain.TextInsertedPayload{{Position: 0, Text: "Hello"}},
			want:    "Hello",
		},
		{
			name: "sequential inserts",
			inserts: []domain.TextInsertedPayload{
				{Position: 0, Text: "Hello"},
				{Position: 5, Text: " World"},
			},
			want: "Hello World",
		},
		{
			name: "insert in the middle",
			inserts: []domain.TextInsertedPayload{
				{Position: 0, Text: "Helo"},
				{Position: 3, Text: "l"},
			},
			want: "Hello",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			docID := uuid.NewString()
			fx.seed(t, docID, domain.EventTypeDocumentCreated, "sys",
				domain.DocumentCreatedPayload{Title: "T"})
			for _, ins := range tc.inserts {
				fx.seed(t, docID, domain.EventTypeTextInserted, "u1", ins)
			}
			state, err := fx.proj.Project(context.Background(), docID)
			if err != nil {
				t.Fatalf("project: %v", err)
			}
			if state.Content != tc.want {
				t.Errorf("content: want %q, got %q", tc.want, state.Content)
			}
		})
	}
}

func Test_Projector_Project_AppliesDeleteEventsCorrectly(t *testing.T) {
	fx := newFixture(t)
	docID := uuid.NewString()

	fx.seed(t, docID, domain.EventTypeDocumentCreated, "sys",
		domain.DocumentCreatedPayload{Title: "T"})
	fx.seed(t, docID, domain.EventTypeTextInserted, "u1",
		domain.TextInsertedPayload{Position: 0, Text: "Hello World"})
	fx.seed(t, docID, domain.EventTypeTextDeleted, "u1",
		domain.TextDeletedPayload{Position: 5, Length: 6})

	state, err := fx.proj.Project(context.Background(), docID)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if state.Content != "Hello" {
		t.Errorf("want %q, got %q", "Hello", state.Content)
	}
}

func Test_Projector_Project_TracksCursorsPerUser(t *testing.T) {
	fx := newFixture(t)
	docID := uuid.NewString()

	fx.seed(t, docID, domain.EventTypeDocumentCreated, "sys",
		domain.DocumentCreatedPayload{Title: "T"})
	fx.seed(t, docID, domain.EventTypeCursorMoved, "alice",
		domain.CursorMovedPayload{Position: 3})
	fx.seed(t, docID, domain.EventTypeCursorMoved, "bob",
		domain.CursorMovedPayload{Position: 7})
	// Alice moves again — latest position wins.
	fx.seed(t, docID, domain.EventTypeCursorMoved, "alice",
		domain.CursorMovedPayload{Position: 5})

	state, err := fx.proj.Project(context.Background(), docID)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if state.Cursors["alice"] != 5 {
		t.Errorf("alice cursor: want 5, got %d", state.Cursors["alice"])
	}
	if state.Cursors["bob"] != 7 {
		t.Errorf("bob cursor: want 7, got %d", state.Cursors["bob"])
	}
}

func Test_Projector_ProjectAt_StopsReplayAtSequence(t *testing.T) {
	fx := newFixture(t)
	docID := uuid.NewString()

	// seq 1: created
	fx.seed(t, docID, domain.EventTypeDocumentCreated, "sys",
		domain.DocumentCreatedPayload{Title: "T"})
	// seq 2–10: insert one 'x' each = after seq 5 we have "xxxx"
	for i := 0; i < 9; i++ {
		fx.seed(t, docID, domain.EventTypeTextInserted, "u1",
			domain.TextInsertedPayload{Position: i, Text: "x"})
	}

	state, err := fx.proj.ProjectAt(context.Background(), docID, 5)
	if err != nil {
		t.Fatalf("projectAt 5: %v", err)
	}
	if state.Version != 5 {
		t.Errorf("want version 5, got %d", state.Version)
	}
	wantLen := 4 // 4 inserts after the created event
	if got := len([]rune(state.Content)); got != wantLen {
		t.Errorf("want %d runes at seq=5, got %d (%q)", wantLen, got, state.Content)
	}
}
