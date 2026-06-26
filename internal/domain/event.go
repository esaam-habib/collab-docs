package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventType identifies the kind of event that occurred on a document.
type EventType string

const (
	// EventTypeDocumentCreated fires when a new document is initialised.
	EventTypeDocumentCreated EventType = "document.created"
	// EventTypeTextInserted fires when characters are inserted into the document.
	EventTypeTextInserted EventType = "text.inserted"
	// EventTypeTextDeleted fires when characters are removed from the document.
	EventTypeTextDeleted EventType = "text.deleted"
	// EventTypeCursorMoved fires when a user moves their caret or changes selection.
	EventTypeCursorMoved EventType = "cursor.moved"
)

// Event is an immutable record of something that happened to a document.
// Events are the source of truth; document state is derived by replaying them.
type Event struct {
	// ID is a unique identifier (UUID) assigned at creation time.
	ID string `json:"id"`
	// DocumentID is the document this event belongs to.
	DocumentID string `json:"documentId"`
	// Type identifies what happened.
	Type EventType `json:"type"`
	// SequenceNumber is assigned by the EventStore; it is 1-based and monotonically
	// increasing per document.
	SequenceNumber int64 `json:"sequenceNumber"`
	// UserID is the user whose action produced this event.
	UserID string `json:"userId"`
	// DisplayName is the human-readable name of the user, included for broadcast.
	DisplayName string `json:"displayName,omitempty"`
	// OccurredAt is the wall-clock time the event was created.
	OccurredAt time.Time `json:"occurredAt"`
	// Payload is the event-type-specific data serialised as JSON.
	Payload json.RawMessage `json:"payload"`
}

// DocumentCreatedPayload is the Payload for EventTypeDocumentCreated.
type DocumentCreatedPayload struct {
	Title string `json:"title"`
}

// TextInsertedPayload is the Payload for EventTypeTextInserted.
type TextInsertedPayload struct {
	Position int    `json:"position"`
	Text     string `json:"text"`
}

// TextDeletedPayload is the Payload for EventTypeTextDeleted.
type TextDeletedPayload struct {
	Position int `json:"position"`
	Length   int `json:"length"`
}

// CursorMovedPayload is the Payload for EventTypeCursorMoved.
type CursorMovedPayload struct {
	// Position is the caret position in Unicode rune units.
	Position int `json:"position"`
	// SelectionEnd is the end of the selection range; equals Position for a bare caret.
	SelectionEnd int `json:"selectionEnd"`
}

// MarshalPayload serialises v into a json.RawMessage suitable for Event.Payload.
func MarshalPayload(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("domain: marshal payload: %w", err)
	}
	return json.RawMessage(b), nil
}

// UnmarshalInsertPayload deserialises a TextInsertedPayload from a raw Event.Payload.
func UnmarshalInsertPayload(raw json.RawMessage) (TextInsertedPayload, error) {
	var p TextInsertedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("domain: unmarshal insert payload: %w", err)
	}
	return p, nil
}

// UnmarshalDeletePayload deserialises a TextDeletedPayload from a raw Event.Payload.
func UnmarshalDeletePayload(raw json.RawMessage) (TextDeletedPayload, error) {
	var p TextDeletedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("domain: unmarshal delete payload: %w", err)
	}
	return p, nil
}

// UnmarshalCursorPayload deserialises a CursorMovedPayload from a raw Event.Payload.
func UnmarshalCursorPayload(raw json.RawMessage) (CursorMovedPayload, error) {
	var p CursorMovedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("domain: unmarshal cursor payload: %w", err)
	}
	return p, nil
}

// UnmarshalCreatedPayload deserialises a DocumentCreatedPayload from a raw Event.Payload.
func UnmarshalCreatedPayload(raw json.RawMessage) (DocumentCreatedPayload, error) {
	var p DocumentCreatedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("domain: unmarshal created payload: %w", err)
	}
	return p, nil
}
