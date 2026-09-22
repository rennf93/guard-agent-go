# Configuration

Everything is the `guardagent.Config` struct; there are no environment
variables inside the agent (your application maps its environment onto the
struct, as the example app does). `guardagent.DefaultConfig()` returns the
documented defaults.

## Required

| Field | Purpose |
|---|---|
| `APIKey` | Ingestion API key, sent as `X-API-Key` (minimum 10 characters) |

## Endpoints and identity

| Field | Default | Purpose |
|---|---|---|
| `Endpoint` | `https://api.guard-core.com` | Ingestion base URL; a trailing `/api/v1` suffix is removed automatically |
| `ProjectID` | empty | Sent as `X-Project-Id` when non-empty |
| `InstallID` / `InstallIDPath` | `~/.guard-agent/install-id` | Persisted agent identity, sent as `X-Agent-Install-Id` |
| `GuardVersion` / `GuardCoreVersion` | empty | Reported as `guard_version` / `guard_core_version` |

## Buffering

| Field | Default | Purpose |
|---|---|---|
| `BufferSize` | 100 | Per-kind queue capacity |
| `FlushInterval` | 30s | Periodic flush cadence and retry backoff base |
| `HighWatermarkRatio` | 0.8 | Combined occupancy that triggers an early flush |
| `MaxConcurrentFlushes` | 1 | In-flight flush cycle bound |
| `Overflow` | `OverflowDrop` | Full-buffer policy (`drop`, `block`, `raise`) |
| `EnableEvents` / `EnableMetrics` | true | Per-kind send switches |

## Network

| Field | Default | Purpose |
|---|---|---|
| `RetryAttempts` | 3 | Retries after a failed attempt (0 disables) |
| `Timeout` | 30s | Per-request HTTP timeout |
| `BackoffFactor` | 1.0 | Exponential retry delay base in seconds |
| `CompressionEnabled` | true | Gzip bodies at or above the threshold |
| `CompressionThreshold` | 1024 | Gzip cutoff in bytes |
| `SigningSecret` | empty | HMAC-SHA256 secret over the uncompressed body |

With `WithHTTPClient` you can supply a custom `*http.Client` (proxies, mTLS,
custom TLS pools); the agent only overrides its timeout when `Timeout` is set.

## Redis persistence

```go
cfg.Redis = &guardagent.RedisConfig{
    URL:    os.Getenv("GUARD_AGENT_REDIS_URL"), // redis:// or rediss://
    Prefix: "guard:agent",                      // default
}
```

Keys are `{Prefix}:{namespace}:{short-key}` with namespaces `agent_events`
and `agent_metrics`. Persistence is optional; without it, a process crash
loses buffered-but-unsent items.
