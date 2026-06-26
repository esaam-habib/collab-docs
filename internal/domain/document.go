package domain

import (
	"fmt"
	"time"
)

// DocumentState represents the current state of a document derived by
// replaying its event log. It is a value type — Apply returns a new copy.
type DocumentState struct {
	// ID is the unique document identifier.
	ID string `json:"id"`
	// Title is the human-readable document title.
	Title string `json:"title"`
	// Content is the current text content of the document.
	Content string `json:"content"`
	// Version equals the SequenceNumber of the last applied event.
	Version int64 `json:"version"`
	// Cursors maps userID to their current rune position.
	Cursors map[string]int `json:"cursors"`
	// CreatedAt is the timestamp of the DocumentCreated event.
	CreatedAt time.Time `json:"createdAt"`
	// UpdatedAt is the timestamp of the most recently applied event.
	UpdatedAt time.Time `json:"updatedAt"`
}

// NewDocument returns a zero-state document ready for event replay.
// No events are applied; call Apply for each event in sequence.
func NewDocument(id, title string, now time.Time) DocumentState {
	return DocumentState{
		ID:        id,
		Title:     title,
		Content:   "",
		Version:   0,
		Cursors:   make(map[string]int),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Apply is a pure function that returns a new DocumentState after applying event.
// It never mutates state. All positions are Unicode rune positions, not byte offsets.
// Returns ErrUnknownEventType if the event type is not recognised.
// Returns ErrInvalidPosition or ErrInvalidLength if the payload violates document bounds.
func Apply(state DocumentState, event Event) (DocumentState, error) {
	next := state
	next.Cursors = copyCursors(state.Cursors)
	next.Version = event.SequenceNumber
	next.UpdatedAt = event.OccurredAt

	switch event.Type {
	case EventTypeDocumentCreated:
		p, err := UnmarshalCreatedPayload(event.Payload)
		if err != nil {
			return state, fmt.Errorf("apply document.created: %w", err)
		}
		next.ID = event.DocumentID
		next.Title = p.Title
		next.CreatedAt = event.OccurredAt

	case EventTypeTextInserted:
		p, err := UnmarshalInsertPayload(event.Payload)
		if err != nil {
			return state, fmt.Errorf("apply text.inserted: %w", err)
		}
		runes := []rune(next.Content)
		if p.Position < 0 || p.Position > len(runes) {
			return state, fmt.Errorf("apply text.inserted pos=%d len=%d: %w",
				p.Position, len(runes), ErrInvalidPosition)
		}
		ins := []rune(p.Text)
		buf := make([]rune, 0, len(runes)+len(ins))
		buf = append(buf, runes[:p.Position]...)
		buf = append(buf, ins...)
		buf = append(buf, runes[p.Position:]...)
		next.Content = string(buf)

	case EventTypeTextDeleted:
		p, err := UnmarshalDeletePayload(event.Payload)
		if err != nil {
			return state, fmt.Errorf("apply text.deleted: %w", err)
		}
		runes := []rune(next.Content)
		if p.Position < 0 || p.Position > len(runes) {
			return state, fmt.Errorf("apply text.deleted pos=%d len=%d: %w",
				p.Position, len(runes), ErrInvalidPosition)
		}
		if p.Position+p.Length > len(runes) {
			return state, fmt.Errorf("apply text.deleted pos=%d del=%d len=%d: %w",
				p.Position, p.Length, len(runes), ErrInvalidLength)
		}
		buf := make([]rune, 0, len(runes)-p.Length)
		buf = append(buf, runes[:p.Position]...)
		buf = append(buf, runes[p.Position+p.Length:]...)
		next.Content = string(buf)

	case EventTypeCursorMoved:
		p, err := UnmarshalCursorPayload(event.Payload)
		if err != nil {
			return state, fmt.Errorf("apply cursor.moved: %w", err)
		}
		next.Cursors[event.UserID] = p.Position

	default:
		return state, fmt.Errorf("apply event type %q: %w", event.Type, ErrUnknownEventType)
	}

	return next, nil
}

// copyCursors returns a fresh map containing all entries from m.
// This ensures Apply never returns a DocumentState sharing map memory with its input.
func copyCursors(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
