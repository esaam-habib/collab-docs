// Package handler provides HTTP and WebSocket handlers.
package handler

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

// contextKey is a package-private type to prevent context key collisions.
type contextKey string

const contextKeyRequestID contextKey = "requestID"

// RequestID attaches a UUID request ID to the request context and sets
// the X-Request-ID response header for correlation in logs and traces.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		ctx := r.Context()
		// Store in context using the typed key to avoid collisions.
		ctx = contextWithRequestID(ctx, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Logger logs method, path, status code, duration, and request ID for every request.
func Logger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(lrw, r)
			logger.Info("http",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", lrw.statusCode),
				slog.Duration("duration", time.Since(start)),
				slog.String("requestID", requestIDFromContext(r.Context())),
			)
		})
	}
}

// Recover catches panics in downstream handlers, logs the stack trace,
// and returns a 500 Internal Server Error to the client.
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("http: panic recovered",
						slog.Any("panic", rec),
						slog.String("stack", string(debug.Stack())),
						slog.String("requestID", requestIDFromContext(r.Context())),
					)
					http.Error(w, http.StatusText(http.StatusInternalServerError),
						http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// loggingResponseWriter wraps http.ResponseWriter to capture the status code.
// It also implements http.Hijacker so WebSocket upgrades work through the
// Logger middleware — gorilla/websocket requires Hijack() to take over the
// raw TCP connection.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Hijack implements http.Hijacker by delegating to the underlying ResponseWriter.
// Required for WebSocket upgrades to work when the Logger middleware is active.
func (lrw *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := lrw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("middleware: underlying ResponseWriter does not implement http.Hijacker")
	}
	return h.Hijack()
}
