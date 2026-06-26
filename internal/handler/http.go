package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/yourusername/collab-docs/internal/config"
	"github.com/yourusername/collab-docs/internal/domain"
	"github.com/yourusername/collab-docs/internal/eventstore"
	"github.com/yourusername/collab-docs/internal/hub"
	"github.com/yourusername/collab-docs/internal/projector"
)

// APIResponse is the standard JSON envelope for all HTTP responses.
type APIResponse[T any] struct {
	Data  T         `json:"data,omitempty"`
	Error *APIError `json:"error,omitempty"`
}

// APIError carries a machine-readable code and a human-readable message.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type HTTPHandler struct {
	projector  *projector.Projector
	store      eventstore.EventStore
	hub        *hub.Hub
	cfg        *config.Config
	logger     *slog.Logger
	serverCtx  context.Context  // ← add this
}

func NewHTTPHandler(
	proj      *projector.Projector,
	store     eventstore.EventStore,
	h         *hub.Hub,
	cfg       *config.Config,
	logger    *slog.Logger,
	serverCtx context.Context,  // ← add this
) *HTTPHandler {
	return &HTTPHandler{
		projector: proj,
		store:     store,
		hub:       h,
		cfg:       cfg,
		logger:    logger,
		serverCtx: serverCtx,  // ← add this
	}
}

// RegisterRoutes registers all HTTP routes on mux.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", h.serveIndex)
	mux.HandleFunc("POST /api/documents", h.createDocument)
	mux.HandleFunc("GET /api/documents/{id}", h.getDocument)
	mux.HandleFunc("GET /api/documents/{id}/history", h.getHistory)
	mux.HandleFunc("GET /ws", h.serveWebSocket)
}

// clientCfg extracts WebSocket timing configuration from the server config.
func (h *HTTPHandler) clientCfg() hub.ClientConfig {
	return hub.ClientConfig{
		WriteWait:       h.cfg.WriteWait,
		PongWait:        h.cfg.PongWait,
		PingInterval:    h.cfg.PingInterval,
		MaxMessageBytes: h.cfg.MaxMessageBytes,
	}
}

// serveIndex serves the web/index.html single-page application.
func (h *HTTPHandler) serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/index.html")
}

// serveWebSocket upgrades the connection and registers the client with the hub.
func (h *HTTPHandler) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	serveWS(h.serverCtx, h.hub, h.clientCfg(), h.logger, w, r)
}

// createDocumentRequest is the request body for POST /api/documents.
type createDocumentRequest struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	UserID string `json:"userId"`
}

// createDocument handles POST /api/documents.
// Body: {"id":"...","title":"...","userId":"..."}
// Response 201: the new DocumentState.
func (h *HTTPHandler) createDocument(w http.ResponseWriter, r *http.Request) {
	var req createDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body")
		return
	}
	if req.Title == "" {
		writeError(w, http.StatusBadRequest, "MISSING_TITLE", "title is required")
		return
	}
	if req.ID == "" {
		req.ID = uuid.NewString()
	}
	if req.UserID == "" {
		req.UserID = "anonymous"
	}

	cmd := domain.CreateDocumentCommand{
		DocumentID: req.ID,
		UserID:     req.UserID,
		Title:      req.Title,
	}
	if err := cmd.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	payload, err := domain.MarshalPayload(domain.DocumentCreatedPayload{Title: req.Title})
	if err != nil {
		h.logger.Error("createDocument: marshal payload", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to build event")
		return
	}

	event := domain.Event{
		ID:         uuid.NewString(),
		DocumentID: req.ID,
		Type:       domain.EventTypeDocumentCreated,
		UserID:     req.UserID,
		OccurredAt: time.Now(),
		Payload:    payload,
	}

	if _, err := h.store.Append(r.Context(), event); err != nil {
		h.logger.Error("createDocument: append", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist event")
		return
	}

	state, err := h.projector.Project(r.Context(), req.ID)
	if err != nil {
		h.logger.Error("createDocument: project", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to project document")
		return
	}

	writeJSON(w, http.StatusCreated, APIResponse[domain.DocumentState]{Data: state})
}

// getDocument handles GET /api/documents/{id}.
// Response 200: the current DocumentState. 404 if not found.
func (h *HTTPHandler) getDocument(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	state, err := h.projector.Project(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrDocumentNotFound) {
			writeError(w, http.StatusNotFound, "DOCUMENT_NOT_FOUND",
				"document "+id+" not found")
			return
		}
		h.logger.Error("getDocument: project", slog.String("id", id), slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, APIResponse[domain.DocumentState]{Data: state})
}

// getHistory handles GET /api/documents/{id}/history[?after=<seq>].
// Returns all events or events after the given sequence number.
func (h *HTTPHandler) getHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var (
		events []domain.Event
		err    error
	)

	if afterStr := r.URL.Query().Get("after"); afterStr != "" {
		after, parseErr := strconv.ParseInt(afterStr, 10, 64)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, "BAD_REQUEST",
				"after must be an integer sequence number")
			return
		}
		events, err = h.store.LoadFrom(r.Context(), id, after)
	} else {
		events, err = h.store.Load(r.Context(), id)
	}

	if err != nil {
		h.logger.Error("getHistory: load", slog.String("id", id), slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	if events == nil {
		events = []domain.Event{}
	}
	writeJSON(w, http.StatusOK, APIResponse[[]domain.Event]{Data: events})
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers already sent; best effort.
		slog.Default().Error("writeJSON: encode", slog.String("err", err.Error()))
	}
}

// writeError writes a structured JSON error response.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, APIResponse[struct{}]{
		Error: &APIError{Code: code, Message: message},
	})
}

// SeedDefaultDocument creates the default document if it does not already exist.
// Called once at server startup.
func SeedDefaultDocument(ctx context.Context, store eventstore.EventStore, docID, title, userID string) error {
	events, err := store.Load(ctx, docID)
	if err != nil {
		return err
	}
	if len(events) > 0 {
		return nil // already seeded
	}
	payload, err := domain.MarshalPayload(domain.DocumentCreatedPayload{Title: title})
	if err != nil {
		return err
	}
	event := domain.Event{
		ID:         uuid.NewString(),
		DocumentID: docID,
		Type:       domain.EventTypeDocumentCreated,
		UserID:     userID,
		OccurredAt: time.Now(),
		Payload:    payload,
	}
	_, err = store.Append(ctx, event)
	return err
}
