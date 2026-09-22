# AGENTS.md
Guidance for AI agents (including Claude Code) working in this repository.

## Project Overview

guard-agent-go is the Go telemetry agent for the Guard ecosystem. It is a framework-agnostic library (single package `guardagent` at the repo root) that buffers security events, metrics, and agent status in memory and ships them to the Guard Core App ingestion API with at-least-once delivery guarantees: nothing acknowledged is lost, nothing unacknowledged is forgotten.

- **Module**: `github.com/rennf93/guard-agent-go`
- **Import path**: `github.com/rennf93/guard-agent-go` (package `guardagent`)
- **Go directive**: 1.25.0
- **License**: MIT
- **Release state**: no tag yet. Version `0.1.0` semantics are implemented and documented; releases are published as `v*` git tags when the maintainer cuts them. Until then pin a commit or track `main`.
- **Only dependency**: `github.com/redis/go-redis/v9` (optional Redis persistence)

## Ecosystem Position

```
guard-core (Python engine) / guard-core-go (Go engine)
├── guard-agent (Python) / guard-agent-rs (Rust) / guard-agent-go (this repo)  <- Telemetry agents
├── fastapi-guard, flaskapi-guard, djapi-guard, tornadoapi-guard               <- Python adapters
└── gin-guard, nethttp-guard                                                   <- Go adapters
```

The agent contains no security-detection logic. Engines and adapters produce events, the agent ships them to the SaaS ingestion API hosted by guard-core-app.

## Architecture

```
guard-agent-go/
├── agent.go            # Agent: per-kind buffers, flush loops, at-least-once handshake, lifecycle
├── config.go           # Config, DefaultConfig, OverflowPolicy, RedisConfig, normalization and validation
├── models.go           # SecurityEvent, SecurityMetric, wire payloads, metric type constants, uuid helpers
├── transport.go        # HTTP transport: gzip, HMAC signing, retry loop, 429/413/4xx/5xx handling, outcomes
├── circuit_breaker.go  # CircuitBreaker: 5 consecutive failures, 60s recovery, half-open probing
├── persistence.go      # Optional Redis durability: persist-on-enqueue, TTL, confirm-delete, startup reload
├── signing.go          # HMAC-SHA256 "v1=" signatures over the uncompressed body
├── install_id.go       # Install identity: override, ~/.guard-agent/install-id file, fail-open
├── errors.go           # Typed errors (errors.Is/As): BufferFullError, ConfigError, PermanentError, RateLimitedError
├── version.go          # Version constant
├── *_test.go           # White-box tests in package guardagent
├── integration_test.go # Redis-backed tests behind the `integration` build tag
├── mock_test.go        # httptest mock of the ingestion contract shared by all tests
├── examples/basic_usage/  # minimal wiring: engine OnBlock hook to agent events (main.go, README.md)
├── mkdocs.yml          # mkdocs-material site definition (docs/ sources)
└── docs/               # documentation site sources: index.md, usage.md, configuration.md
```

Key invariants an agent must preserve when editing:

1. The handshake is always drain, then send, then confirm (delete persisted records) or requeue in the original order; under buffer pressure during requeue the TAIL (newest items) is evicted.
2. The HMAC signature covers the UNCOMPRESSED JSON body even when the wire bytes are gzipped; the server verifies after decompression.
3. No exported method may panic out or block the host beyond the documented overflow policy; telemetry failures are logged and counted.
4. Per-kind state (queues, failure streaks, backoff gates) stays independent: one kind failing must never stall the other.
5. Redis failures are fail-open: log, count, keep going.
6. Community workflows (`issue-link`, `stale`, `sync-labels`) stay byte-identical to the Go family's (gin-guard and siblings); `greetings`, `summary`, and `labeler`/`labels` carry repo-specific text (agent subsystems, not adapter middleware) and must not drift in structure.

## Quick Start

```sh
go test ./...
REDIS_HOST=127.0.0.1 go test -tags integration ./...
```

Minimal usage:

```go
cfg := guardagent.DefaultConfig()
cfg.APIKey = "your-ingest-api-key"
cfg.Endpoint = "https://your-guard-core-app.example.com"
cfg.ProjectID = "your-project-id"

agent, err := guardagent.New(cfg)
if err != nil {
	log.Fatal(err)
}
if err := agent.Start(context.Background()); err != nil {
	log.Fatal(err)
}
defer agent.Stop(context.Background())

_ = agent.SendEvent(ctx, guardagent.SecurityEvent{
	EventType: "penetration_attempt",
	IPAddress: r.RemoteAddr,
	Reason:    "suspicious pattern",
})
```

## Configuration

Start from `DefaultConfig()` and override. Three fields are zero-honest (the zero value means what it says): `RetryAttempts` 0 disables retries, `CompressionThreshold` 0 compresses every body, and the booleans `EnableEvents`, `EnableMetrics`, `CompressionEnabled` are false in a hand-built struct.

| Field | Default | Notes |
| --- | --- | --- |
| `APIKey` | required | Ingestion credential, min 10 chars (`X-API-Key`) |
| `Endpoint` | `https://api.guard-core.com` | Trailing slash and `/api/v1` suffix stripped |
| `ProjectID` | empty | Sent as `X-Project-Id` when set |
| `BufferSize` | 100 | Per-kind queue capacity |
| `FlushInterval` | 30s | Periodic flush cadence and backoff base |
| `StatusInterval` | 300s | Status report cadence, minimum 60s |
| `HighWatermarkRatio` | 0.8 | Early flush when combined occupancy reaches this share |
| `MaxConcurrentFlushes` | 1 | Wake-driven flusher goroutine count |
| `Overflow` | drop | `drop`, `block`, or `raise` |
| `RetryAttempts` | 3 (via DefaultConfig) | Zero-honest: 0 disables retries |
| `Timeout` | 30s | Per-request HTTP timeout |
| `BackoffFactor` | 1.0 | Base of the retry backoff |
| `CompressionEnabled` / `CompressionThreshold` | true / 1024 | gzip bodies at or above the threshold |
| `SigningSecret` | empty | Enables `X-Payload-Signature` |
| `InstallID` / `InstallIDPath` | auto | Override or relocate `~/.guard-agent/install-id` |
| `Redis` | nil | `URL`, `Prefix` (default `guard:agent`), `TTL` (default 1h) |
| `GuardVersion` / `GuardCoreVersion` | empty | Reported to the ingestion API |

## Reliability Semantics

| Situation | Behavior |
| --- | --- |
| Buffer full, policy `drop` (default) | Oldest same-kind item evicted, its Redis record confirmed, drop logged every 100th drop |
| Buffer full, policy `block` | Sender waits (500ms poll, wake on drain) until space frees or ctx ends; requeues always keep their slots |
| Buffer full, policy `raise` | `SendEvent`/`SendMetric` return `*BufferFullError` (`errors.Is` `ErrBufferFull`) |
| Flush trigger | Combined occupancy >= `BufferSize * HighWatermarkRatio`, or `FlushInterval` elapsed |
| 200 success | Confirm: persisted records deleted, counters updated, streak reset |
| 200 with `success:false` or `errors[]` | Partial failure: whole batch requeued in original order (duplicates possible, at-least-once) |
| 429 | Honor `Retry-After` (default 60s when absent or invalid, clamped to 300s), then retry |
| 413 | Recursive split-or-drop: halve the batch; a singleton that still 413s is durably dropped, never requeued |
| 400 / 404 / 422 | Permanent rejection: batch dropped durably, records confirmed, never requeued |
| 401 / 403 / 5xx / network error | Retried with `BackoffFactor * 2^attempt`, capped at 60s |
| Per-kind failure streak | Gate flushes for `min(FlushInterval * 2^(streak-1), 300s)`; success resets |
| Circuit breaker | Opens after 5 consecutive transport failures, probes after 60s (half-open); permanent rejections and 413s are exempt |
| Degraded status | Breaker open, combined occupancy >= 90%, or lifetime failure rate > 10% |
| Redis persistence | Write on enqueue with TTL, delete on confirm, reload on `Start`; any Redis failure is fail-open (logged + counted, plus a 30s write cooldown after 3 consecutive failures) |
| Stop | Cancels loops, then one final forced flush bypassing the gates |
| Panic anywhere | Recovered, logged, reported as `ErrInternal`; in-flight batches are requeued |

## Development Commands

There is no Makefile. Every command below comes from the CI workflows or this file.

| Command | Purpose |
| --- | --- |
| `go build ./...` | Compile |
| `gofmt -l .` | List unformatted files (CI fails if non-empty); `gofmt -w .` to fix |
| `go vet ./...` | Vet |
| `go test ./...` | Unit tests |
| `REDIS_HOST=127.0.0.1 go test -tags integration ./...` | Full suite incl. Redis-backed tests (skips without `REDIS_HOST`) |
| `go install golang.org/x/vuln/cmd/govulncheck@latest && govulncheck ./...` | Vulnerability scan (must report zero) |
| `pip install mkdocs-material && mkdocs build --strict` | Build the documentation site (CI deploys it on master) |

On hosts without a Go toolchain, run the same commands in Docker: `docker run --rm -v "$PWD":/app -w /app -e GOTOOLCHAIN=auto golang:1.25-alpine sh -c "go test ./..."`, with a `redis:7-alpine` container on a shared Docker network (network alias `redis`, `REDIS_HOST=redis`) for the integration build.

## Testing Guidelines

- Stdlib `testing` only, no testify. Plain `if` checks with `t.Fatalf("... got %v want %v", ...)`.
- White-box style: tests live in `package guardagent` next to the source.
- `mock_test.go` implements the ingestion contract (gzip decompression, post-decompression signature verification, 413 at the size limit, partial-failure 200s) so transport tests assert against the real server semantics.
- Timing-sensitive tests poll (`waitUntil`, bounded loops) instead of assuming a specific call reaches the server, because per-kind backoff gates legitimately swallow flush attempts.
- Integration tests require the `integration` build tag plus `REDIS_HOST`; without it they `t.Skip`. They use the shared key prefix `guard_agent_go_test` and clean up via scan-delete.
- New behavior needs a test that would fail without it; keep the suite race-clean (`go test -race`) and deterministic.

## Best Practices

- Never push to `main` directly and never push `v*` tags; a tag push triggers the Release Gate workflow. Work on feature branches and PRs.
- Conventional commits (`feat:`, `test:`, `docs:`, `ci:`, `fix:`), no AI attribution in commit messages or PR bodies.
- Do not add web framework dependencies; this package stays protocol-level (net/http + go-redis only).
- If govulncheck flags an indirect dependency, record the fix as an explicit indirect floor in `go.mod` with the GO id in the commit message (gin-guard precedent).
- Preserve the failure-isolation policy when touching exported methods: recover panics, return errors, never take the host application down.
- Documentation edits go to README.md and AGENTS.md; CLAUDE.md must stay byte-identical to AGENTS.md (`cmp AGENTS.md CLAUDE.md`).

## Related Projects

- [guard-core](https://github.com/rennf93/guard-core): the framework-agnostic Python engine that anchors the ecosystem.
- [guard-core-go](https://github.com/rennf93/guard-core-go): the Go engine whose adapters emit the telemetry this agent ships.
- [guard-agent](https://github.com/rennf93/guard-agent): the Python reference agent; this repo mirrors its reliability semantics.
- [guard-agent-rs](https://github.com/rennf93/guard-agent-rs): the Rust sibling agent.
- [gin-guard](https://github.com/rennf93/gin-guard) and [nethttp-guard](https://github.com/rennf93/nethttp-guard): Go adapters that produce the events.
- [guard-core-app](https://github.com/rennf93/guard-core-app): hosts the ingestion API this agent reports to.
