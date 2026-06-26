package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yourusername/collab-docs/internal/hub"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func serveWS(
	ctx context.Context,
	h *hub.Hub,
	clientCfg hub.ClientConfig,
	logger *slog.Logger,
	w http.ResponseWriter,
	r *http.Request,
) {
	userID      := r.URL.Query().Get("userId")
	documentID  := r.URL.Query().Get("documentId")
	displayName := r.URL.Query().Get("displayName")

	if userID == "" || documentID == "" {
		http.Error(w, "userId and documentId are required", http.StatusBadRequest)
		return
	}
	if displayName == "" {
		displayName = userID
	}

	// Clear the HTTP server's WriteTimeout before upgrading.
	// A WebSocket connection is long-lived; the server-level WriteTimeout
	// would otherwise kill it after WriteTimeout seconds.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		logger.Warn("ws: clear write deadline", slog.String("err", err.Error()))
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Warn("ws: upgrade failed", slog.String("err", err.Error()))
		return
	}

	client := hub.NewClient(userID, displayName, documentID, conn, h, clientCfg, logger)
	h.Register(client)

	go client.WritePump(ctx)
	go client.ReadPump(ctx)
}