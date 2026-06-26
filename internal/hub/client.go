// Package hub manages WebSocket connections and event broadcasting.
package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// MessageType is the discriminant on every server-to-client message envelope.
type MessageType string

const (
	// MessageTypeSnapshot delivers the full document state on first connection.
	MessageTypeSnapshot MessageType = "snapshot"
	// MessageTypeEvent delivers a confirmed, sequenced event to apply.
	MessageTypeEvent MessageType = "event"
	// MessageTypeAck confirms the client's command was accepted and stored.
	MessageTypeAck MessageType = "ack"
	// MessageTypeError reports that a client command was rejected.
	MessageTypeError MessageType = "error"
	// MessageTypeUserJoined notifies clients that a new user has connected.
	MessageTypeUserJoined MessageType = "user.joined"
	// MessageTypeUserLeft notifies clients that a user has disconnected.
	MessageTypeUserLeft MessageType = "user.left"
)

// ServerMessage is the envelope for every message sent from the server to a client.
type ServerMessage struct {
	Type    MessageType `json:"type"`
	Payload any         `json:"payload"`
}

// ClientMessage pairs a raw inbound WebSocket payload with the sending Client.
type ClientMessage struct {
	Client  *Client
	Payload []byte
}

// ClientConfig holds WebSocket timing parameters extracted from the server Config.
type ClientConfig struct {
	WriteWait       time.Duration
	PongWait        time.Duration
	PingInterval    time.Duration
	MaxMessageBytes int64
}

// Client represents one WebSocket connection belonging to one user on one document.
// The send channel is the only shared state; it is protected by sendMu + closed.
type Client struct {
	// id is the unique connection identifier (UUID), distinct from userID.
	id          string
	userID      string
	displayName string
	documentID  string
	conn        *websocket.Conn
	// send is the outbound message queue. Write via safeSend; close via closeSend.
	send   chan []byte
	sendMu sync.Mutex
	closed bool

	hub    *Hub
	cfg    ClientConfig
	logger *slog.Logger
}

// NewClient creates a Client for conn. The caller is responsible for
// calling hub.Register(client) and starting ReadPump/WritePump goroutines.
func NewClient(
	userID, displayName, documentID string,
	conn *websocket.Conn,
	hub *Hub,
	cfg ClientConfig,
	logger *slog.Logger,
) *Client {
	return &Client{
		id:          uuid.NewString(),
		userID:      userID,
		displayName: displayName,
		documentID:  documentID,
		conn:        conn,
		send:        make(chan []byte, 256),
		hub:         hub,
		cfg:         cfg,
		logger:      logger,
	}
}

// ID returns the unique connection identifier.
func (c *Client) ID() string { return c.id }

// UserID returns the user identifier.
func (c *Client) UserID() string { return c.userID }

// DisplayName returns the human-readable name.
func (c *Client) DisplayName() string { return c.displayName }

// DocumentID returns the document this client is editing.
func (c *Client) DocumentID() string { return c.documentID }

// ReadPump pumps messages from the WebSocket connection to the hub's inbound channel.
// Owner: the HTTP handler goroutine that calls this. Exits on context cancellation,
// connection close, or read error. Calls hub.Unregister before returning.
func (c *Client) ReadPump(ctx context.Context) {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(c.cfg.MaxMessageBytes)
	if err := c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait)); err != nil {
		c.logger.Warn("client: set read deadline", slog.String("id", c.id), slog.String("err", err.Error()))
	}
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
	})

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Warn("client: unexpected close",
					slog.String("id", c.id), slog.String("err", err.Error()))
			}
			return
		}

		select {
		case c.hub.inbound <- ClientMessage{Client: c, Payload: msg}:
		case <-ctx.Done():
			return
		}
	}
}

// WritePump pumps messages from the send channel to the WebSocket connection.
// Owner: the HTTP handler goroutine that calls this. Exits on context cancellation,
// send channel close, or write error. Sends a proper close frame before returning.
func (c *Client) WritePump(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteWait)); err != nil {
				return
			}
			if !ok {
				// Hub closed the send channel.
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				c.logger.Warn("client: write error",
					slog.String("id", c.id), slog.String("err", err.Error()))
				return
			}

		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteWait)); err != nil {
				return
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Warn("client: ping error",
					slog.String("id", c.id), slog.String("err", err.Error()))
				return
			}

		case <-ctx.Done():
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteWait)); err != nil {
				return
			}
			_ = c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutdown"))
			return
		}
	}
}

// sendMessage serialises msg and queues it for delivery. Thread-safe.
// If the send buffer is full or the client is already closed, the message is dropped.
func (c *Client) sendMessage(msg ServerMessage) {
	b, err := json.Marshal(msg)
	if err != nil {
		c.logger.Error("client: marshal message", slog.String("err", err.Error()))
		return
	}
	c.safeSend(b)
}

// safeSend enqueues b for delivery. It is safe to call concurrently and after closeSend.
func (c *Client) safeSend(b []byte) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.send <- b:
	default:
		c.logger.Warn("client: send buffer full, dropping message", slog.String("id", c.id))
	}
}

// closeSend closes the send channel exactly once. Thread-safe.
func (c *Client) closeSend() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.send)
	}
}

// sendError is a convenience wrapper for sending a structured error to the client.
func (c *Client) sendError(code, message string) {
	c.sendMessage(ServerMessage{
		Type: MessageTypeError,
		Payload: map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

// commandMessage is the shape of every message a client sends over WebSocket.
type commandMessage struct {
	Type         string `json:"type"`
	DocumentID   string `json:"documentId"`
	UserID       string `json:"userId"`
	DisplayName  string `json:"displayName"`
	Position     int    `json:"position"`
	SelectionEnd int    `json:"selectionEnd"`
	Text         string `json:"text"`
	Length       int    `json:"length"`
	Title        string `json:"title"`
	BaseVersion  int64  `json:"baseVersion"` // ← add this
}

// parseCommandMessage deserialises the raw WebSocket payload into a commandMessage.
func parseCommandMessage(data []byte) (commandMessage, error) {
	var msg commandMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return commandMessage{}, fmt.Errorf("hub: parse command: %w", err)
	}
	return msg, nil
}
