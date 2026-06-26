// Package projector builds read models by replaying events from the event store.
// This is the query side of CQRS.
package projector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/yourusername/collab-docs/internal/domain"
	"github.com/yourusername/collab-docs/internal/eventstore"
)

// Projector replays event logs from the store to produce document read models.
type Projector struct {
	store  eventstore.EventStore
	logger *slog.Logger
}

// New creates a Projector backed by the given EventStore.
func New(store eventstore.EventStore, logger *slog.Logger) *Projector {
	return &Projector{store: store, logger: logger}
}

// Project loads all events for documentID and replays them to return current state.
// Returns domain.ErrDocumentNotFound if no events exist for the documentID.
func (p *Projector) Project(ctx context.Context, documentID string) (domain.DocumentState, error) {
	events, err := p.store.Load(ctx, documentID)
	if err != nil {
		return domain.DocumentState{}, fmt.Errorf("projector: load %q: %w", documentID, err)
	}
	if len(events) == 0 {
		return domain.DocumentState{}, fmt.Errorf("projector: %w", domain.ErrDocumentNotFound)
	}

	p.logger.Debug("projector: replaying events",
		slog.String("documentID", documentID),
		slog.Int("count", len(events)),
	)

	state, err := replay(events)
	if err != nil {
		return domain.DocumentState{}, fmt.Errorf("projector: replay %q: %w", documentID, err)
	}
	state.ID = documentID
	return state, nil
}

// ProjectAt replays events up to and including atSequence.
// Returns domain.ErrDocumentNotFound if atSequence is 0 or no events exist.
func (p *Projector) ProjectAt(ctx context.Context, documentID string, atSequence int64) (domain.DocumentState, error) {
	if atSequence == 0 {
		return domain.DocumentState{}, fmt.Errorf("projector: atSequence=0: %w", domain.ErrDocumentNotFound)
	}

	events, err := p.store.Load(ctx, documentID)
	if err != nil {
		return domain.DocumentState{}, fmt.Errorf("projector: load %q: %w", documentID, err)
	}
	if len(events) == 0 {
		return domain.DocumentState{}, fmt.Errorf("projector: %w", domain.ErrDocumentNotFound)
	}

	var filtered []domain.Event
	for _, e := range events {
		if e.SequenceNumber <= atSequence {
			filtered = append(filtered, e)
		}
	}

	p.logger.Debug("projector: replaying events at sequence",
		slog.String("documentID", documentID),
		slog.Int64("atSequence", atSequence),
		slog.Int("count", len(filtered)),
	)

	state, err := replay(filtered)
	if err != nil {
		return domain.DocumentState{}, fmt.Errorf("projector: replay %q at %d: %w", documentID, atSequence, err)
	}
	state.ID = documentID
	return state, nil
}

// replay applies events in order to a zero DocumentState, returning the final state.
func replay(events []domain.Event) (domain.DocumentState, error) {
	state := domain.DocumentState{Cursors: make(map[string]int)}
	for _, e := range events {
		var err error
		state, err = domain.Apply(state, e)
		if err != nil {
			return domain.DocumentState{}, fmt.Errorf("apply seq=%d type=%s: %w",
				e.SequenceNumber, e.Type, err)
		}
	}
	return state, nil
}
