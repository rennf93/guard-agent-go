# guard-agent-go

`guard-agent-go` is the Go telemetry agent of the Guard ecosystem. It
buffers security events, metrics, and status reports produced by your
application (typically through a guard-core-go adapter's block hooks) and
ships them to the
[guard-core-app](https://github.com/rennf93/guard-core-app) ingestion API
with at-least-once delivery.

It is a port of the normative [guard-agent](https://github.com/rennf93/guard-agent)
(Python) semantics: per-kind buffers, periodic and watermark-driven flushes,
overflow policies, retry with backoff, 413 batch split-or-drop, Retry-After
honoring, a circuit breaker, optional Redis-backed queue persistence, and a
persisted install id.

## Installation

```bash
go get github.com/rennf93/guard-agent-go
```

Requires Go 1.25 or later.

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rennf93/guard-agent-go"
)

func main() {
	agent, err := guardagent.New(guardagent.Config{
		APIKey:         os.Getenv("GUARD_AGENT_API_KEY"),
		ProjectID:      os.Getenv("GUARD_AGENT_PROJECT_ID"),
		SigningSecret:  os.Getenv("GUARD_AGENT_SIGNING_SECRET"),
		GuardVersion:   "v0.1.0",
		GuardCoreVersion: "v0.1.0",
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = agent.Stop(shutdown)
	}()

	_ = agent.SendEvent(ctx, guardagent.SecurityEvent{
		Timestamp:   time.Now().UTC(),
		EventType:   "rate_limited",
		IPAddress:   "203.0.113.7",
		ActionTaken: "BLOCKED",
		Reason:      "endpoint rate limit exceeded",
		Endpoint:    "/api",
		Method:      "GET",
	})

	<-ctx.Done()
}
```

Most applications do not call the agent directly: guard-core-go adapters
expose a block hook (for example `SecurityConfig.OnBlock`) where the event
construction belongs. See
[guard-core-go](https://github.com/rennf93/guard-core-go) for the engine and
the adapter repositories for wiring examples.

## What the agent guarantees

- At-least-once delivery: buffered items survive crashes when Redis
  persistence is enabled and are requeued on partial batch failure
- HMAC request signing: `X-Payload-Signature` covers the uncompressed body,
  matching the ingestion API's post-decompression verification
- Fail-soft: ingestion outages never panic or block the application beyond
  the chosen overflow policy; the circuit breaker sheds load when the API is
  down
