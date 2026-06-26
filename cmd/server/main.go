package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourusername/collab-docs/internal/config"
	"github.com/yourusername/collab-docs/internal/eventstore"
	"github.com/yourusername/collab-docs/internal/handler"
	"github.com/yourusername/collab-docs/internal/hub"
	"github.com/yourusername/collab-docs/internal/projector"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config: load failed", slog.String("err", err.Error()))
		os.Exit(1)
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	logger.Info("server starting", slog.String("port", cfg.Port))

	store := eventstore.NewInMemoryStore(logger)
	proj  := projector.New(store, logger)
	h     := hub.NewHub(store, proj, logger)

	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := handler.SeedDefaultDocument(seedCtx, store, "default", "Welcome Document", "system"); err != nil {
		logger.Error("seed default document", slog.String("err", err.Error()))
		seedCancel()
		os.Exit(1)
	}
	seedCancel()

	// ctx must be created before HTTPHandler so it can be passed in.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	httpHandler := handler.NewHTTPHandler(proj, store, h, cfg, logger, ctx)

	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	var wrappedMux http.Handler = mux
	wrappedMux = handler.RequestID(wrappedMux)
	wrappedMux = handler.Logger(logger)(wrappedMux)
	wrappedMux = handler.Recover(logger)(wrappedMux)

	srv := &http.Server{
		Addr:        ":" + cfg.Port,
		Handler:     wrappedMux,
		IdleTimeout: cfg.IdleTimeout,
	}

	go h.Run(ctx)

	go func() {
		logger.Info("http server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server", slog.String("err", err.Error()))
			cancel()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown", slog.String("err", err.Error()))
	}
	logger.Info("shutdown complete")
}
