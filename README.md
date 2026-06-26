# CollabDocs

> A Google Docs-style real-time collaborative document editor built from scratch in Go — no frameworks, no ORMs, just clean architecture and raw WebSockets.



---

## What is this?

CollabDocs lets multiple users edit the same document simultaneously in real time. Every keystroke from every user is instantly visible to all others — complete with **named cursor overlays**, **live user presence**, and a full **event history with time travel**.

Built on **Event Sourcing**: the document is never mutated directly. Every user action produces an immutable event appended to an ordered log. The current state is always derived by replaying that log — giving you a full audit trail and time-travel debugging for free.

---

## Features

| Feature | Description |
|---|---|
| **Real-time sync** | Changes appear instantly across all connected browsers |
| **Named cursors** | See exactly where every user's caret is, labelled with their name |
| **User presence** | Live sidebar showing who's online and their cursor position |
| **Event history** | Every keystroke is recorded as an immutable event |
| **Time travel** | Drag a slider to replay the document to any point in its past |
| **Operational Transformation** | Server-side OT resolves concurrent edits correctly |
| **Reconnect** | Exponential backoff reconnect (500ms → 30s) on connection loss |
| **Resync** | One-click resync to recover from any divergence |
| **Multi-document** | Each `?doc=<id>` URL is an independent document |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Browser (Vanilla JS)                  │
│  contenteditable div  │  cursor overlay  │  history panel   │
└───────────────────────────────┬─────────────────────────────┘
                                │ WebSocket (JSON commands)
┌───────────────────────────────▼─────────────────────────────┐
│                         Go HTTP Server                        │
│                                                               │
│  ┌──────────┐    ┌─────────────┐    ┌────────────────────┐  │
│  │  Handler  │───▶│     Hub     │───▶│   EventStore       │  │
│  │ (HTTP/WS) │    │ (fan-out)   │    │ (append-only log)  │  │
│  └──────────┘    └──────┬──────┘    └────────────────────┘  │
│                          │                    │               │
│                   ┌──────▼──────┐    ┌────────▼───────────┐ │
│                   │   Projector  │    │   Domain / OT      │ │
│                   │  (CQRS read) │    │ (Apply + Transform) │ │
│                   └─────────────┘    └────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Key design decisions

- **Event Sourcing** — document state is derived by replaying an immutable event log. No UPDATE statements, no lost history.
- **CQRS** — the `Projector` (read side) replays events independently of the `Hub` (write side).
- **Server-authoritative OT** — the server transforms every incoming operation against concurrent events before storing, so simultaneous edits always converge correctly.
- **No global state** — everything injected through constructors. Zero `init()` functions.
- **No frameworks** — `net/http`, `log/slog`, `sync`, `context` from the standard library only.

---

## Project Structure

```
collab-docs/
├── cmd/server/          # Entry point — wires everything together
├── internal/
│   ├── config/          # Environment-based configuration
│   ├── domain/          # Core types: Event, Command, DocumentState
│   │   ├── errors.go    # Sentinel errors
│   │   ├── event.go     # Event types and payload marshalers
│   │   ├── document.go  # Pure Apply() function — no mutation
│   │   └── command.go   # Command types with validation
│   ├── eventstore/      # Append-only in-memory event log
│   ├── projector/       # Replays events → DocumentState (CQRS read side)
│   ├── hub/             # WebSocket connection manager + broadcaster
│   │   ├── hub.go       # Fan-out loop, OT transform, command dispatch
│   │   └── client.go    # Per-connection read/write pumps
│   └── handler/         # HTTP routes, middleware, WS upgrade
├── web/
│   └── index.html       # Entire frontend — vanilla JS, no dependencies
├── go.mod
└── go.sum
```

---

## Tech Stack

**Backend**
- [Go 1.22](https://go.dev) — standard library only for HTTP, JSON, sync, logging
- [gorilla/websocket](https://github.com/gorilla/websocket) — WebSocket implementation
- [google/uuid](https://github.com/google/uuid) — UUID generation

**Frontend**
- Zero dependencies — pure HTML, CSS, vanilla JavaScript
- `contenteditable` div with a `pointer-events: none` overlay for cursor rendering
- `Range` + `TreeWalker` APIs for pixel-perfect cursor positioning
- `ResizeObserver` to keep the cursor layer in sync with the editor

---

## Getting Started

### Prerequisites

- Go 1.22 or higher — [install here](https://go.dev/dl/)

### Run locally

```bash
# Clone the repo
git clone https://github.com/esaam-habib/collab-docs.git
cd collab-docs

# Download dependencies
go mod tidy

# Start the server
go run ./cmd/server
```

Open `http://localhost:8080?doc=default` in two or more browser tabs.

### Configuration

All configuration is via environment variables. Defaults work out of the box.

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP server port |
| `LOG_LEVEL` | `info` | Log verbosity: debug / info / warn / error |
| `MAX_MESSAGE_BYTES` | `65536` | Max WebSocket message size |
| `PING_INTERVAL` | `30s` | WebSocket ping frequency |
| `PONG_WAIT` | `60s` | Max time to wait for pong before closing |
| `WRITE_WAIT` | `10s` | Timeout for each WebSocket write |
| `IDLE_TIMEOUT` | `120s` | HTTP keep-alive idle timeout |

```bash
# Example: run with debug logging
LOG_LEVEL=debug go run ./cmd/server
```

---

## Running Tests

```bash
# Run all tests
go test ./...

# With race detector (recommended)
go test -race ./...

# Verbose output
go test -race -v ./internal/...
```

**22 tests across 3 packages — all passing with `-race`.**

Test highlights:
- `ConcurrentAppends_NoDuplicateSequenceNumbers` — 50 goroutines × 20 appends, zero duplicate sequence numbers
- `ProjectAt_StopsReplayAtSequence` — time travel correctness
- HTTP handler tests using `httptest` — no network required

---

## Deployment

### Docker

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o server ./cmd/server

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .
COPY web/ ./web/
EXPOSE 8080
CMD ["./server"]
```

```bash
docker build -t collab-docs .
docker run -p 8080:8080 collab-docs
```

### Railway (recommended free hosting)

1. Push to GitHub
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Add environment variable: `MAX_MESSAGE_BYTES=65536`
4. Settings → Networking → Generate Domain

Your app is live. Railway auto-deploys on every `git push`.

---

## How It Works — The Collaboration Model

```
User types "A"
     │
     ▼
input event fires
     │
     ├── update local state.content immediately (optimistic)
     └── send { type: "insertText", position: 5, text: "A", baseVersion: 42 }
                                                                    │
                                                      Server receives command
                                                                    │
                                                      Transform against events
                                                      since version 42
                                                                    │
                                                      Append to event log
                                                      (assigned seq: 43)
                                                                    │
                                                      Broadcast event to ALL
                                                      clients on this document
                                                                    │
                              ┌─────────────────────────────────────┘
                              │
                    Own client receives echo
                    → skip (already applied locally)

                    Remote clients receive event
                    → apply to their content + adjust their cursors
```

---

## Known Limitations

- **In-memory storage** — documents are lost on server restart. Swap `InMemoryStore` for a Postgres/Redis backend to persist.
- **Single server** — the Hub runs in one process. For horizontal scaling, replace the in-memory subscriber fan-out with Redis Pub/Sub.
- **Basic OT** — handles concurrent inserts and deletes correctly for plain text. Does not handle complex cases like simultaneous replace operations on identical ranges.

---

## Contributing

```bash
# Fork and clone
git clone https://github.com/your-username/collab-docs.git

# Create a feature branch
git checkout -b feat/your-feature

# Make changes, run tests
go test -race ./...

# Push and open a PR
git push origin feat/your-feature
```

---
## **Live Link** - https://collab-docs.up.railway.app


<div align="center">
  Built with Go 
</div>
