# guard-agent-go

Go telemetry agent for the [Guard ecosystem](https://github.com/rennf93/guard-core). Buffers security events, metrics, and agent status locally and ships them to the Guard Core App ingestion API with at-least-once delivery: nothing acknowledged is lost, nothing unacknowledged is forgotten.

The agent contains no detection logic. Guard engines and their adapters produce the telemetry; this library transports it, with the same reliability semantics as the Python [guard-agent](https://github.com/rennf93/guard-agent) and its [Rust sibling](https://github.com/rennf93/guard-agent-rs).

## Status

Implemented and functional. Version `0.1.0` semantics are complete and covered by the test suite. There is no release tag yet, so pin a commit (or track `main`) until the first tag is published; releases will be cut as `v*` git tags.

## Features

- Per-kind queues (events, metrics) with size and time flush triggers and a high-watermark early flush.
- Overflow policies: `drop` (default), `block`, `raise`.
- At-least-once handshake: drain, send, then confirm or requeue in the original order.
- gzip request bodies and an HMAC-SHA256 `v1=` signature that covers the **uncompressed** body, which is what the server verifies after decompression.
- Retry with exponential backoff, `Retry-After` honoring (capped at 300s), 413 recursive split-or-drop, 400/404/422 permanent rejection, 200-partial requeue, per-kind failure-streak backoff, and a 5-failure / 60s circuit breaker.
- Optional Redis persistence: write-on-enqueue with a TTL, delete-on-confirm, reload on `Start`, fail-open on any Redis error.
- Stable install identity persisted to `~/.guard-agent/install-id`.
- Failure isolation: no exported method panics out or blocks the host beyond the configured overflow policy.

## Install

```sh
go get github.com/rennf93/guard-agent-go/v3@v3.0.2
```

Package name is `guardagent`; the module is `github.com/rennf93/guard-agent-go/v3`.

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/rennf93/guard-agent-go/v3"
)

func main() {
	cfg := guardagent.DefaultConfig()
	cfg.APIKey = "your-ingest-api-key"
	cfg.ProjectID = "your-project-id"
	// cfg.Endpoint = "https://your-guard-core-app.example.com"

	agent, err := guardagent.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = agent.Stop(context.Background()) }()

	ctx := context.Background()
	if err := agent.SendEvent(ctx, guardagent.SecurityEvent{
		EventType: "penetration_attempt",
		IPAddress: "203.0.113.7",
		Method:    "GET",
		Endpoint:  "/admin",
		Reason:    "suspicious pattern",
	}); err != nil {
		log.Printf("telemetry: %v", err)
	}
	_ = agent.SendMetric(ctx, guardagent.SecurityMetric{
		MetricType: guardagent.MetricRequestCount,
		Value:      1,
	})
}
```

Redis durability is one field away and never makes the agent depend on Redis being up:

```go
cfg.Redis = &guardagent.RedisConfig{URL: "redis://localhost:6379"}
cfg.SigningSecret = os.Getenv("INGEST_PAYLOAD_SIGNING_SECRET")
```

## Reliability semantics

| Situation | Behavior |
| --- | --- |
| Buffer full, `drop` (default) | Evict the oldest item of that kind, confirm its persisted record, log every 100th drop |
| Buffer full, `block` | Wait for a flush to free space (returns `ctx.Err()` if the context ends first) |
| Buffer full, `raise` | `SendEvent`/`SendMetric` return `*BufferFullError` (`errors.Is(err, guardagent.ErrBufferFull)`) |
| Flush trigger | Combined occupancy at or above `BufferSize * HighWatermarkRatio`, or `FlushInterval` elapsed |
| 200 | Confirm: delete persisted records, reset the failure streak |
| 200 with `success:false` or `errors[]` | Requeue the whole batch in original order (at-least-once: duplicates possible, losses are not) |
| 429 | Honor `Retry-After` (60s default, 300s cap) and retry |
| 413 | Halve the batch and send both halves; a singleton that still 413s is dropped durably |
| 400 / 404 / 422 | Permanent: drop durably, never requeue |
| 401 / 403 / 5xx / network | Retry with `BackoffFactor * 2^attempt` (60s cap) |
| Repeated per-kind failure | Gate that kind for `min(FlushInterval * 2^(streak-1), 300s)` |
| 5 consecutive transport failures | Circuit breaker opens for 60s, then admits one probe (half-open); permanent rejections and 413s are exempt |
| `Status()` | `healthy`, `degraded` (breaker open, 90% occupancy, or >10% lifetime failure rate), or `failed` after `Stop` |
| Redis unavailable | Log, count, keep going; writes pause for 30s after 3 consecutive failures; records expire by TTL |
| `Stop` | Cancel the loops, then one final flush that bypasses the backoff gates |

### The signature covers the uncompressed body

The ingestion API verifies `X-Payload-Signature` **after** its gzip middleware decompresses the request (`telemetry_router.py`, `payload_signature.py`), so the signature must be an HMAC-SHA256 over the uncompressed JSON. This agent signs before compression and sends `v1=<hex>` accordingly.

The Python and TypeScript agents sign the post-compression wire bytes instead, so their signatures stop verifying as soon as gzip kicks in. That mismatch is a known defect on their side; this agent intentionally does not reproduce it, and the test suite asserts the server-side semantic with a mock that decompresses first and verifies second.

## Configuration

Start from `guardagent.DefaultConfig()` and override. `RetryAttempts` and `CompressionThreshold` are zero-honest (0 means zero retries, and 0 compresses every body); the feature booleans are false in a hand-built `Config{}`.

| Field | Default | Notes |
| --- | --- | --- |
| `APIKey` | required | At least 10 characters |
| `Endpoint` | `https://api.guard-core.com` | Trailing `/` and `/api/v1` are stripped |
| `ProjectID` | empty | Sent as `X-Project-Id` |
| `BufferSize` | 100 | Per-kind capacity |
| `FlushInterval` | 30s | Flush cadence and backoff base |
| `StatusInterval` | 300s | Minimum 60s |
| `HighWatermarkRatio` | 0.8 | Early flush threshold |
| `MaxConcurrentFlushes` | 1 | Wake-driven flushers |
| `Overflow` | `drop` | `drop`, `block`, `raise` |
| `RetryAttempts` | 3 via `DefaultConfig()` | 0 disables retries |
| `Timeout` | 30s | Per request |
| `BackoffFactor` | 1.0 | Seconds, base of `2^attempt` |
| `CompressionEnabled` / `CompressionThreshold` | true / 1024 | gzip at or above the threshold |
| `SigningSecret` | empty | Enables `X-Payload-Signature` |
| `InstallID` / `InstallIDPath` | auto / `~/.guard-agent/install-id` | Override either |
| `Redis` | nil | `URL`, `Prefix` (`guard:agent`), `TTL` (1h) |
| `GuardVersion` / `GuardCoreVersion` | empty | Reported to the API |

## Development

```sh
go build ./...
gofmt -l .
go vet ./...
go test ./...
REDIS_HOST=127.0.0.1 go test -tags integration ./...
go install golang.org/x/vuln/cmd/govulncheck@latest && govulncheck ./...
```

The integration build skips itself when `REDIS_HOST` is unset. On hosts without a Go toolchain, run the same commands in `golang:1.25-alpine` with a `redis:7-alpine` container reachable as `redis` on a shared Docker network.

## Links

- [guard-core](https://github.com/rennf93/guard-core): the Python engine that anchors the ecosystem.
- [guard-agent](https://github.com/rennf93/guard-agent) / [guard-agent-rs](https://github.com/rennf93/guard-agent-rs): Python and Rust sibling agents.
- [guard-core-app](https://github.com/rennf93/guard-core-app): hosts the ingestion API.
- [gin-guard](https://github.com/rennf93/gin-guard) / [nethttp-guard](https://github.com/rennf93/nethttp-guard): Go adapters that emit the events.

## License

MIT
