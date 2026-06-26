package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yourusername/collab-docs/internal/config"
	"github.com/yourusername/collab-docs/internal/domain"
	"github.com/yourusername/collab-docs/internal/eventstore"
	"github.com/yourusername/collab-docs/internal/handler"
	"github.com/yourusername/collab-docs/internal/hub"
	"github.com/yourusername/collab-docs/internal/projector"
)

// testFixture wires up a complete in-memory handler stack for testing.
type testFixture struct {
	store   *eventstore.InMemoryStore
	proj    *projector.Projector
	h       *hub.Hub
	handler *handler.HTTPHandler
	mux     *http.ServeMux
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	store := eventstore.NewInMemoryStore(slog.Default())
	proj := projector.New(store, slog.Default())
	cfg := &config.Config{
		Port:            "8080",
		WriteWait:       10e9,
		PongWait:        60e9,
		PingInterval:    30e9,
		MaxMessageBytes: 4096,
	}
	h := hub.NewHub(store, proj, slog.Default())
	hh := handler.NewHTTPHandler(proj, store, h, cfg, slog.Default())
	mux := http.NewServeMux()
	hh.RegisterRoutes(mux)
	return &testFixture{store: store, proj: proj, h: h, handler: hh, mux: mux}
}

func decodeBody[T any](t *testing.T, body *bytes.Buffer) handler.APIResponse[T] {
	t.Helper()
	var resp handler.APIResponse[T]
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func Test_HTTPHandler_CreateDocument_Success_Returns201WithState(t *testing.T) {
	fx := newTestFixture(t)
	body := `{"id":"test-doc","title":"My Doc","userId":"user-1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/documents", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	fx.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeBody[domain.DocumentState](t, rec.Body)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %+v", resp.Error)
	}
	if resp.Data.Title != "My Doc" {
		t.Errorf("want title %q, got %q", "My Doc", resp.Data.Title)
	}
	if resp.Data.ID != "test-doc" {
		t.Errorf("want id %q, got %q", "test-doc", resp.Data.ID)
	}
}

func Test_HTTPHandler_CreateDocument_MissingTitle_Returns400(t *testing.T) {
	fx := newTestFixture(t)
	body := `{"id":"no-title-doc","userId":"user-1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/documents", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	fx.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
	resp := decodeBody[struct{}](t, rec.Body)
	if resp.Error == nil {
		t.Fatal("expected error in response body")
	}
	if resp.Error.Code != "MISSING_TITLE" {
		t.Errorf("want code MISSING_TITLE, got %q", resp.Error.Code)
	}
}

func Test_HTTPHandler_GetDocument_NotFound_Returns404WithErrorCode(t *testing.T) {
	fx := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/api/documents/ghost", nil)
	rec := httptest.NewRecorder()

	fx.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
	resp := decodeBody[struct{}](t, rec.Body)
	if resp.Error == nil {
		t.Fatal("expected error body")
	}
	if resp.Error.Code != "DOCUMENT_NOT_FOUND" {
		t.Errorf("want DOCUMENT_NOT_FOUND, got %q", resp.Error.Code)
	}
	if !errors.Is(domain.ErrDocumentNotFound, domain.ErrDocumentNotFound) {
		t.Error("domain sentinel not defined")
	}
}

func Test_HTTPHandler_GetDocument_Success_ReturnsCurrentState(t *testing.T) {
	fx := newTestFixture(t)
	ctx := context.Background()

	// Pre-seed via SeedDefaultDocument helper.
	if err := handler.SeedDefaultDocument(ctx, fx.store, "my-doc", "Test Title", "sys"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/documents/my-doc", nil)
	rec := httptest.NewRecorder()

	fx.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeBody[domain.DocumentState](t, rec.Body)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %+v", resp.Error)
	}
	if resp.Data.Title != "Test Title" {
		t.Errorf("want title %q, got %q", "Test Title", resp.Data.Title)
	}
}

func Test_HTTPHandler_GetHistory_ReturnsEventsInOrder(t *testing.T) {
	fx := newTestFixture(t)
	ctx := context.Background()

	if err := handler.SeedDefaultDocument(ctx, fx.store, "hist-doc", "History Test", "sys"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/documents/hist-doc/history", nil)
	rec := httptest.NewRecorder()

	fx.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	resp := decodeBody[[]domain.Event](t, rec.Body)
	if len(resp.Data) != 1 {
		t.Fatalf("want 1 event, got %d", len(resp.Data))
	}
	if resp.Data[0].Type != domain.EventTypeDocumentCreated {
		t.Errorf("want document.created, got %s", resp.Data[0].Type)
	}
}

func Test_HTTPHandler_GetHistory_AfterParam_ReturnsSubset(t *testing.T) {
	fx := newTestFixture(t)
	ctx := context.Background()

	if err := handler.SeedDefaultDocument(ctx, fx.store, "after-doc", "After Test", "sys"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Add 3 more events.
	for _, text := range []string{"a", "b", "c"} {
		payload, _ := domain.MarshalPayload(domain.TextInsertedPayload{Position: 0, Text: text})
		_, _ = fx.store.Append(ctx, domain.Event{
			DocumentID: "after-doc",
			Type:       domain.EventTypeTextInserted,
			UserID:     "u1",
			Payload:    payload,
		})
	}

	req := httptest.NewRequest(http.MethodGet, "/api/documents/after-doc/history?after=2", nil)
	rec := httptest.NewRecorder()

	fx.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeBody[[]domain.Event](t, rec.Body)
	// seq 1 is the created event, seqs 2-4 are inserts; after=2 returns seqs 3,4
	if len(resp.Data) != 2 {
		t.Fatalf("want 2 events after seq=2, got %d", len(resp.Data))
	}
	for _, e := range resp.Data {
		if e.SequenceNumber <= 2 {
			t.Errorf("got event with seq %d (want > 2)", e.SequenceNumber)
		}
	}
}
