package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yourusername/collab-docs/internal/domain"
	"github.com/yourusername/collab-docs/internal/eventstore"
	"github.com/yourusername/collab-docs/internal/projector"
)

// Hub maintains the set of active WebSocket clients and routes events to them.
// It is the fan-out layer between the event store and connected browsers.
//
// Concurrency model:
//   - A single Run goroutine owns the clients and docUnsubs maps.
//   - All mutations to those maps happen only inside Run via channel messages.
//   - broadcastToDocument acquires a read lock to snapshot the client set, then
//     sends to each client's channel outside any lock.
//   - Goroutines spawned by Run only communicate back via the broadcast channel or
//     directly to individual client send channels.
type Hub struct {
	// mu protects clients and docUnsubs. Write-locked in handleRegister/handleUnregister
	// (called from Run). Read-locked in broadcastToDocument (called from goroutines).
	mu        sync.RWMutex
	clients   map[string]map[*Client]bool // documentID → set of clients
	docUnsubs map[string]func()           // documentID → unsubscribe function

	register   chan *Client
	unregister chan *Client
	// inbound receives raw client messages for command dispatch.
	inbound chan ClientMessage

	store     eventstore.EventStore
	projector *projector.Projector
	logger    *slog.Logger
	// done is closed when Run exits; goroutines owned by Run select on it.
	done chan struct{}
}

// NewHub creates a Hub with the given dependencies.
func NewHub(store eventstore.EventStore, proj *projector.Projector, logger *slog.Logger) *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]bool),
		docUnsubs:  make(map[string]func()),
		register:   make(chan *Client, 16),
		unregister: make(chan *Client, 16),
		inbound:    make(chan ClientMessage, 256),
		store:      store,
		projector:  proj,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// Register queues a client for addition to the hub. Non-blocking.
func (h *Hub) Register(c *Client) {
	h.register <- c
}

// Unregister queues a client for removal from the hub. Non-blocking.
func (h *Hub) Unregister(c *Client) {
	h.unregister <- c
}

// Run is the hub's event loop. It must be called in exactly one goroutine.
// Owner: cmd/server/main.go.
// Exit: ctx cancellation.
func (h *Hub) Run(ctx context.Context) {
	defer close(h.done)
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-h.register:
			h.handleRegister(ctx, c)
		case c := <-h.unregister:
			h.handleUnregister(c)
		case msg := <-h.inbound:
			// Dispatch command processing to a goroutine so Run never blocks on I/O.
			go h.handleCommand(ctx, msg)
		}
	}
}

// handleRegister adds a client to the hub, subscribes to the document's event stream
// if not already subscribed, and delivers the initial snapshot asynchronously.
// Called only from Run.
func (h *Hub) handleRegister(ctx context.Context, c *Client) {
	docID := c.documentID

	h.mu.Lock()
	if h.clients[docID] == nil {
		h.clients[docID] = make(map[*Client]bool)
	}
	h.clients[docID][c] = true
	_, alreadySubscribed := h.docUnsubs[docID]
	h.mu.Unlock()

	if !alreadySubscribed {
		ch, unsub := h.store.Subscribe(docID)
		h.mu.Lock()
		h.docUnsubs[docID] = unsub
		h.mu.Unlock()
		// Owner: exits when ch is closed (by unsub) or when h.done is closed.
		go h.listenAndBroadcast(docID, ch)
	}

	// Deliver initial snapshot and user.joined without blocking Run.
	go func() {
		state, err := h.projector.Project(ctx, docID)
		if err != nil {
			c.sendError("SNAPSHOT_FAILED", fmt.Sprintf("load document: %v", err))
			return
		}
		c.sendMessage(ServerMessage{Type: MessageTypeSnapshot, Payload: state})

		// Broadcast user.joined to all clients on this document.
		h.broadcastToDocument(docID, ServerMessage{
			Type: MessageTypeUserJoined,
			Payload: map[string]string{
				"userId":      c.userID,
				"displayName": c.displayName,
			},
		})
	}()
}

// handleUnregister removes a client from the hub and broadcasts user.left.
// If no clients remain on a document, the event store subscription is cancelled.
// Called only from Run.
func (h *Hub) handleUnregister(c *Client) {
	docID := c.documentID
	userID := c.userID
	displayName := c.displayName

	h.mu.Lock()
	if clients, ok := h.clients[docID]; ok {
		if _, exists := clients[c]; exists {
			delete(clients, c)
			c.closeSend()
		}
		if len(clients) == 0 {
			delete(h.clients, docID)
			if unsub, ok := h.docUnsubs[docID]; ok {
				unsub()
				delete(h.docUnsubs, docID)
			}
		}
	}
	h.mu.Unlock()

	// Broadcast user.left after releasing the lock.
	h.broadcastToDocument(docID, ServerMessage{
		Type: MessageTypeUserLeft,
		Payload: map[string]string{
			"userId":      userID,
			"displayName": displayName,
		},
	})
}

// listenAndBroadcast fans out events from an event store subscription to all
// clients currently editing docID.
// Owner: handleRegister. Exit: ch closed (unsub called) or h.done closed.
func (h *Hub) listenAndBroadcast(docID string, ch <-chan domain.Event) {
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			h.broadcastToDocument(docID, ServerMessage{Type: MessageTypeEvent, Payload: e})
		case <-h.done:
			return
		}
	}
}

// broadcastToDocument sends msg to every client currently editing docID.
// It acquires a read lock only long enough to copy the client set, then sends
// to each client's channel outside the lock to prevent deadlocks.
func (h *Hub) broadcastToDocument(docID string, msg ServerMessage) {
	b, err := json.Marshal(msg)
	if err != nil {
		h.logger.Error("hub: marshal broadcast", slog.String("err", err.Error()))
		return
	}

	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients[docID]))
	for c := range h.clients[docID] {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		c.safeSend(b)
	}
}

// handleCommand parses the raw payload from a ClientMessage, builds a domain
// Command, validates it, converts it to an Event, appends it to the store,
// and sends an ack. On any failure, sends a structured error to the originating client.
// Owner: goroutine spawned by Run.
func (h *Hub) handleCommand(ctx context.Context, msg ClientMessage) {
	raw, err := parseCommandMessage(msg.Payload)
	if err != nil {
		msg.Client.sendError("PARSE_ERROR", fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	cmd, err := h.buildCommand(raw, msg.Client)
	if err != nil {
		msg.Client.sendError("UNKNOWN_COMMAND", fmt.Sprintf("unknown command type %q", raw.Type))
		return
	}

	if err := cmd.Validate(); err != nil {
		msg.Client.sendError("VALIDATION_ERROR", err.Error())
		return
	}

	// OT: transform the command against all events that occurred after
	// the client's base version before building the event.
	transformedCmd, err := h.transformCommand(ctx, cmd, raw.BaseVersion)
	if err != nil {
		msg.Client.sendError("TRANSFORM_ERROR", fmt.Sprintf("transform: %v", err))
		return
	}

	event, err := h.buildEvent(transformedCmd, raw)
	if err != nil {
		msg.Client.sendError("BUILD_ERROR", fmt.Sprintf("build event: %v", err))
		return
	}

	saved, err := h.store.Append(ctx, event)
	if err != nil {
		msg.Client.sendError("STORE_ERROR", fmt.Sprintf("persist event: %v", err))
		return
	}

	msg.Client.sendMessage(ServerMessage{
		Type: MessageTypeAck,
		Payload: map[string]any{
			"eventId":        saved.ID,
			"sequenceNumber": saved.SequenceNumber,
		},
	})
}

// transformCommand applies operational transformation to cmd against all
// events that happened after baseVersion, adjusting text positions so the
// operation applies correctly to the current document state.
func (h *Hub) transformCommand(ctx context.Context, cmd domain.Command, baseVersion int64) (domain.Command, error) {
	// Only insert and delete need position transformation.
	switch c := cmd.(type) {
	case domain.InsertTextCommand:
		pos, err := h.transformPosition(ctx, c.DocumentID, c.Position, len([]rune(c.Text)), true, baseVersion)
		if err != nil {
			return cmd, err
		}
		c.Position = pos
		return c, nil
	case domain.DeleteTextCommand:
		pos, err := h.transformPosition(ctx, c.DocumentID, c.Position, c.Length, false, baseVersion)
		if err != nil {
			return cmd, err
		}
		c.Position = pos
		return c, nil
	default:
		return cmd, nil
	}
}

// transformPosition shifts position against all concurrent events after baseVersion.
// isInsert=true means we are transforming an insert; false means a delete.
func (h *Hub) transformPosition(ctx context.Context, docID string, position, length int, isInsert bool, baseVersion int64) (int, error) {
	concurrent, err := h.store.LoadFrom(ctx, docID, baseVersion)
	if err != nil {
		return position, fmt.Errorf("load concurrent events: %w", err)
	}

	for _, e := range concurrent {
		switch e.Type {
		case domain.EventTypeTextInserted:
			p, err := domain.UnmarshalInsertPayload(e.Payload)
			if err != nil {
				continue
			}
			insertLen := len([]rune(p.Text))
			if p.Position <= position {
				position += insertLen
			}
		case domain.EventTypeTextDeleted:
			p, err := domain.UnmarshalDeletePayload(e.Payload)
			if err != nil {
				continue
			}
			if p.Position < position {
				shift := min(p.Length, position-p.Position)
				position -= shift
			}
		}
	}
	return position, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// buildCommand converts a raw commandMessage into a typed domain.Command.
func (h *Hub) buildCommand(raw commandMessage, c *Client) (domain.Command, error) {
	dn := raw.DisplayName
	if dn == "" {
		dn = c.displayName
	}
	switch domain.CommandType(raw.Type) {
	case domain.CommandTypeInsertText:
		return domain.InsertTextCommand{
			DocumentID: c.documentID, UserID: c.userID, DisplayName: dn,
			Position: raw.Position, Text: raw.Text,
		}, nil
	case domain.CommandTypeDeleteText:
		return domain.DeleteTextCommand{
			DocumentID: c.documentID, UserID: c.userID, DisplayName: dn,
			Position: raw.Position, Length: raw.Length,
		}, nil
	case domain.CommandTypeMoveCursor:
		return domain.MoveCursorCommand{
			DocumentID: c.documentID, UserID: c.userID, DisplayName: dn,
			Position: raw.Position, SelectionEnd: raw.SelectionEnd,
		}, nil
	case domain.CommandTypeCreateDocument:
		return domain.CreateDocumentCommand{
			DocumentID: c.documentID, UserID: c.userID, DisplayName: dn,
			Title: raw.Title,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %s", domain.ErrUnknownCommandType, raw.Type)
	}
}

// buildEvent converts a validated domain.Command into an unsequenced domain.Event.
func (h *Hub) buildEvent(cmd domain.Command, raw commandMessage) (domain.Event, error) {
	var (
		eventType   domain.EventType
		payload     json.RawMessage
		err         error
		displayName string
	)

	switch c := cmd.(type) {
	case domain.InsertTextCommand:
		eventType = domain.EventTypeTextInserted
		displayName = c.DisplayName
		payload, err = domain.MarshalPayload(domain.TextInsertedPayload{Position: c.Position, Text: c.Text})
	case domain.DeleteTextCommand:
		eventType = domain.EventTypeTextDeleted
		displayName = c.DisplayName
		payload, err = domain.MarshalPayload(domain.TextDeletedPayload{Position: c.Position, Length: c.Length})
	case domain.MoveCursorCommand:
		eventType = domain.EventTypeCursorMoved
		displayName = c.DisplayName
		payload, err = domain.MarshalPayload(domain.CursorMovedPayload{
			Position:     c.Position,
			SelectionEnd: c.SelectionEnd,
		})
	case domain.CreateDocumentCommand:
		eventType = domain.EventTypeDocumentCreated
		displayName = c.DisplayName
		payload, err = domain.MarshalPayload(domain.DocumentCreatedPayload{Title: c.Title})
	default:
		return domain.Event{}, fmt.Errorf("%w: %T", domain.ErrUnknownCommandType, cmd)
	}
	if err != nil {
		return domain.Event{}, fmt.Errorf("marshal payload: %w", err)
	}

	return domain.Event{
		ID:          uuid.NewString(),
		DocumentID:  cmd.GetDocumentID(),
		Type:        eventType,
		UserID:      cmd.GetUserID(),
		DisplayName: displayName,
		OccurredAt:  time.Now(),
		Payload:     payload,
	}, nil
}
