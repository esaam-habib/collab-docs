package domain

import "fmt"

// CommandType identifies the kind of command a client is requesting.
type CommandType string

const (
	// CommandTypeInsertText requests inserting text at a position.
	CommandTypeInsertText CommandType = "insertText"
	// CommandTypeDeleteText requests removing a span of text.
	CommandTypeDeleteText CommandType = "deleteText"
	// CommandTypeMoveCursor requests updating the cursor / selection for a user.
	CommandTypeMoveCursor CommandType = "moveCursor"
	// CommandTypeCreateDocument requests creating a new document.
	CommandTypeCreateDocument CommandType = "createDocument"
)

// Command represents a user's intent to change a document.
// Commands are validated and converted to Events by the command handler.
// Implementations must be value types; they carry no mutable state.
type Command interface {
	// Type returns the discriminant for this command.
	Type() CommandType
	// GetDocumentID returns the document this command targets.
	GetDocumentID() string
	// GetUserID returns the user originating this command.
	GetUserID() string
	// Validate checks domain-level invariants that do not require document state.
	Validate() error
}

// InsertTextCommand requests inserting Text at Position (zero-based rune index).
type InsertTextCommand struct {
	DocumentID  string
	UserID      string
	DisplayName string
	Position    int
	Text        string
}

// Type implements Command.
func (c InsertTextCommand) Type() CommandType { return CommandTypeInsertText }

// GetDocumentID implements Command.
func (c InsertTextCommand) GetDocumentID() string { return c.DocumentID }

// GetUserID implements Command.
func (c InsertTextCommand) GetUserID() string { return c.UserID }

// Validate implements Command.
func (c InsertTextCommand) Validate() error {
	if c.Text == "" {
		return fmt.Errorf("InsertTextCommand: %w", ErrEmptyText)
	}
	if c.Position < 0 {
		return fmt.Errorf("InsertTextCommand: %w", ErrInvalidPosition)
	}
	return nil
}

// DeleteTextCommand requests removing Length runes starting at Position.
type DeleteTextCommand struct {
	DocumentID  string
	UserID      string
	DisplayName string
	Position    int
	Length      int
}

// Type implements Command.
func (c DeleteTextCommand) Type() CommandType { return CommandTypeDeleteText }

// GetDocumentID implements Command.
func (c DeleteTextCommand) GetDocumentID() string { return c.DocumentID }

// GetUserID implements Command.
func (c DeleteTextCommand) GetUserID() string { return c.UserID }

// Validate implements Command.
func (c DeleteTextCommand) Validate() error {
	if c.Length <= 0 {
		return fmt.Errorf("DeleteTextCommand: %w", ErrInvalidLength)
	}
	if c.Position < 0 {
		return fmt.Errorf("DeleteTextCommand: %w", ErrInvalidPosition)
	}
	return nil
}

// MoveCursorCommand requests updating the cursor position for a user.
type MoveCursorCommand struct {
	DocumentID   string
	UserID       string
	DisplayName  string
	Position     int
	SelectionEnd int
}

// Type implements Command.
func (c MoveCursorCommand) Type() CommandType { return CommandTypeMoveCursor }

// GetDocumentID implements Command.
func (c MoveCursorCommand) GetDocumentID() string { return c.DocumentID }

// GetUserID implements Command.
func (c MoveCursorCommand) GetUserID() string { return c.UserID }

// Validate implements Command.
func (c MoveCursorCommand) Validate() error {
	if c.Position < 0 {
		return fmt.Errorf("MoveCursorCommand: %w", ErrInvalidPosition)
	}
	return nil
}

// CreateDocumentCommand requests creating a new document with the given title.
type CreateDocumentCommand struct {
	DocumentID  string
	UserID      string
	DisplayName string
	Title       string
}

// Type implements Command.
func (c CreateDocumentCommand) Type() CommandType { return CommandTypeCreateDocument }

// GetDocumentID implements Command.
func (c CreateDocumentCommand) GetDocumentID() string { return c.DocumentID }

// GetUserID implements Command.
func (c CreateDocumentCommand) GetUserID() string { return c.UserID }

// Validate implements Command.
func (c CreateDocumentCommand) Validate() error {
	if c.DocumentID == "" {
		return fmt.Errorf("CreateDocumentCommand: documentID must not be empty")
	}
	if c.Title == "" {
		return fmt.Errorf("CreateDocumentCommand: title must not be empty")
	}
	return nil
}
