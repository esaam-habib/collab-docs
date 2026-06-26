// Package domain contains the core business logic and types.
// It has no dependencies on any other package in this module.
package domain

import "errors"

// Sentinel errors for the domain layer. Use errors.Is for inspection.
var (
	// ErrDocumentNotFound is returned when a document ID has no events in the store.
	ErrDocumentNotFound = errors.New("document not found")
	// ErrInvalidPosition is returned when an operation references a position outside the document.
	ErrInvalidPosition = errors.New("position out of document bounds")
	// ErrInvalidLength is returned when a delete operation would exceed the document content.
	ErrInvalidLength = errors.New("delete length exceeds document content")
	// ErrEmptyText is returned when an insert command contains an empty string.
	ErrEmptyText = errors.New("insert text must not be empty")
	// ErrEventSequenceGap is returned when replayed events have non-consecutive sequence numbers.
	ErrEventSequenceGap = errors.New("event sequence number gap detected")
	// ErrUnknownEventType is returned when Apply receives an unrecognised EventType.
	ErrUnknownEventType = errors.New("unknown event type")
	// ErrUnknownCommandType is returned when a command dispatcher receives an unrecognised CommandType.
	ErrUnknownCommandType = errors.New("unknown command type")
	// ErrStoreAlreadyExists is returned when trying to create a document that already exists.
	ErrStoreAlreadyExists = errors.New("document already exists in store")
)
