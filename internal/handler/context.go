package handler

import "context"

// contextWithRequestID stores the request ID in ctx under the typed key.
func contextWithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKeyRequestID, id)
}

// requestIDFromContext retrieves the request ID stored by RequestID middleware.
// Returns an empty string if not set.
func requestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKeyRequestID).(string); ok {
		return v
	}
	return ""
}
