---
name: guard-agent-go
description: Use when adding telemetry reporting to a Go service or Go Guard adapter, or when working in github.com/rennf93/guard-agent-go: build the agent with guardagent.New(guardagent.DefaultConfig()) plus APIKey/Endpoint/ProjectID, call Start/Stop for the background flush and status loops, buffer with SendEvent/SendMetric and force delivery with Flush, pick the buffer overflow policy (drop, block, raise), understand the at-least-once handshake (drain, send, confirm or requeue in original order), the HMAC-SHA256 v1= signature over the UNCOMPRESSED body that the server verifies after gzip decompression, Retry-After honoring capped at 300s, 413 recursive split-or-drop, 400/404/422 permanent rejection, the 5-failure 60s circuit breaker, per-kind failure-streak backoff, optional Redis persistence with TTL and startup reload, and the failure-isolation policy (no exported method panics out or blocks the host), plus testing via the mock ingestion server, REDIS_HOST, and go test -tags integration.
---

# guard-agent-go

Go telemetry agent for the [Guard ecosystem](https://github.com/rennf93/guard-core). Ships security events, metrics, and agent status to the Guard Core App ingestion API with at-least-once delivery. It contains no security-detection logic: engines and adapters produce the events, the agent ships them. Module: `github.com/rennf93/guard-agent-go` (package `guardagent`), Go `1.25.0`, no release tag yet.

## Quick Reference

| Identifier | Kind | Notes |
| --- | --- | --- |
| `guardagent.New(cfg, opts...)` | func | Validates config, resolves the install id, never panics; options `WithLogger`, `WithHTTPClient` |
| `guardagent.DefaultConfig()` | func | Full defaults (100-item buffers, 30s flush, 300s status, 0.8 watermark, drop, 3 retries, gzip at 1024) |
| `agent.Start(ctx)` / `agent.Stop(ctx)` | methods | Start is idempotent; Stop cancels loops then does one final forced flush |
| `agent.SendEvent(ctx, ev)` / `agent.SendMetric(ctx, m)` | methods | Buffer one item; works before Start |
| `agent.Flush(ctx)` | method | Drain and send both kinds now; returns the transport error of any failed kind |
| `agent.Status()` / `agent.Healthy()` / `agent.Stats()` | methods | Snapshot, health check, lifetime counters |
| `guardagent.OverflowDrop` / `OverflowBlock` / `OverflowRaise` | consts | Full-buffer policy; drop is the default |
| `guardagent.ErrBufferFull` / `ErrClosed` / `ErrCircuitOpen` / `ErrInvalidEvent` / `ErrInternal` | sentinels | Use `errors.Is` |
| `guardagent.BufferFullError` / `ConfigError` / `PermanentError` / `RateLimitedError` | types | Use `errors.As` |
| `guardagent.MetricRequestCount` and 6 siblings | consts | Allowed `MetricType` values |
| `guardagent.StatusHealthy` / `StatusDegraded` / `StatusFailed` | consts | `Status().State` vocabulary |

Ingestion contract: `POST /api/v1/events`, `/api/v1/metrics`, `/api/v1/status` with `X-API-Key`, optional `X-Project-Id`, and `X-Agent-Install-Id`; optional gzip body; optional `X-Payload-Signature: v1=<hmac-sha256 hex over the UNCOMPRESSED body>`.

## Installation

There is no release tag yet; pin a commit or track `main`:

```sh
go get github.com/rennf93/guard-agent-go@main
```

## Setup

```go
cfg := guardagent.DefaultConfig()
cfg.APIKey = "your-ingest-api-key"
cfg.ProjectID = "your-project-id"

agent, err := guardagent.New(cfg)
if err != nil {
	log.Fatal(err)
}
if err := agent.Start(context.Background()); err != nil {
	log.Fatal(err)
}
defer agent.Stop(context.Background())
```

Redis durability (optional, fail-open) and signing:

```go
cfg.Redis = &guardagent.RedisConfig{URL: "redis://localhost:6379", Prefix: "guard:agent"}
cfg.SigningSecret = os.Getenv("INGEST_PAYLOAD_SIGNING_SECRET")
```

## Reliability Semantics

- Flushes fire when combined occupancy reaches `BufferSize * HighWatermarkRatio` or every `FlushInterval`.
- Confirm or requeue: a 200 confirms (deletes persisted records); a 200 with `success:false` / non-empty `errors[]` requeues the whole batch in its original order, so duplicates are possible and losses are not.
- 413 splits the batch in half recursively; a single item that still 413s is durably dropped.
- 400/404/422 are permanent: the batch is dropped durably and never requeued. 401/403/5xx/network errors retry with `BackoffFactor * 2^attempt` (cap 60s).
- 429 honors `Retry-After` (default 60s, cap 300s). Each kind gates its own flushes for `min(FlushInterval * 2^(streak-1), 300s)` after a failure.
- The circuit breaker opens after 5 consecutive transport failures and probes again after 60s; permanent rejections and 413s never count.
- `Status()` reports `degraded` when the breaker is open, occupancy is at or above 90%, or the lifetime failure rate exceeds 10%.
- Redis persistence writes on enqueue with a TTL (default 1h), deletes on confirm, and reloads on `Start`; Redis failures are logged and counted, never fatal.

## Footguns

- The signature MUST cover the uncompressed JSON body. The Python and TypeScript agents sign the post-gzip wire bytes, which fails server verification whenever gzip applies; this agent deliberately signs before compression.
- `SendEvent` and `SendMetric` cannot report delivery: they only buffer. Inspect `Stats()` or `Status()` and call `Flush` when you need confirmation.
- Hand-built `Config{}` values are not `DefaultConfig()`: `EnableEvents`, `EnableMetrics`, and `CompressionEnabled` default to false, and `RetryAttempts` 0 means zero retries.
- Under `OverflowRaise`, `SendEvent` returns `*BufferFullError`; under `OverflowBlock` it waits until ctx ends. Drop, the default, never blocks and never fails.
- Values in `SecurityEvent.Metadata` must be JSON-serializable; a value that fails to marshal keeps the batch requeued on every cycle.
- The per-kind backoff gate makes an immediate `Flush` a silent no-op after a failure. Poll rather than assume a call reached the server.
- Do not push to `main` and do not push `v*` tags; a tag push triggers the Release Gate workflow.

## Related Projects

- [guard-core](https://github.com/rennf93/guard-core) and [guard-core-go](https://github.com/rennf93/guard-core-go): the engines that anchor the ecosystem.
- [guard-agent](https://github.com/rennf93/guard-agent) (Python) and [guard-agent-rs](https://github.com/rennf93/guard-agent-rs) (Rust): the sibling agents this one mirrors.
- [guard-core-app](https://github.com/rennf93/guard-core-app): hosts the ingestion API.
- [gin-guard](https://github.com/rennf93/gin-guard) and [nethttp-guard](https://github.com/rennf93/nethttp-guard): Go adapters that emit the events.
