# Usage

## Lifecycle

```go
agent, err := guardagent.New(cfg)   // validates config, restores install id
agent.Start(ctx)                    // starts flush and status loops
agent.SendEvent(ctx, ev)            // enqueue a security event
agent.SendMetric(ctx, m)            // enqueue a security metric
agent.Flush(ctx)                    // force a flush cycle
agent.Status()                      // current agent status snapshot
agent.Healthy()                     // true when the ingestion API is reachable
agent.Stats()                       // buffer occupancy, drops, retries
agent.Stop(ctx)                     // final flush, stop loops, close Redis
```

`New` rejects an empty or too-short `APIKey` with an error. `Start` is
idempotent-guarded; `Stop` flushes what fits and confirms persisted records
for everything it successfully sends.

## Events and metrics

`SecurityEvent` and `SecurityMetric` mirror the ingestion API's
`BatchTelemetryRequest` payload. `Timestamp` must be UTC; `IdempotencyKey`
(optional but recommended) deduplicates retries server-side. Field-by-field
descriptions live on the struct definitions in `models.go`.

## Buffering and overflow

Each kind (events, metrics) has its own buffer (`BufferSize`, default 100).
When a buffer fills, `Overflow` decides:

| Policy | Behavior |
|---|---|
| `OverflowDrop` (default) | evicts the oldest item of the same kind, counts a drop |
| `OverflowBlock` | waits for a flush to free a slot; durability over the new writer |
| `OverflowRaise` | returns `*BufferFullError` without buffering |

A combined occupancy at or above `HighWatermarkRatio` (default 0.8) triggers
an early flush.

## Delivery semantics

- Flushes run on `FlushInterval` (default 30s), on watermark, and on demand
- A failed batch retries up to `RetryAttempts` with exponential backoff
  (`BackoffFactor`), honoring `Retry-After` on 429
- A 413 splits the batch or drops its oldest item rather than retrying a
  permanently oversized payload
- 400, 404, and 422 are permanent: the batch is dropped, not retried
- A 200 with `success: false` requeues exactly the failed items
- The circuit breaker opens after consecutive failures and half-opens to
  probe recovery

## Signing

When `SigningSecret` is set, every request carries
`X-Payload-Signature: v1=<hex hmac-sha256>`. The signature covers the
UNCOMPRESSED JSON body; the server verifies after decompression. Gzip
(`CompressionEnabled`) only affects the wire bytes.

## Redis persistence

With `RedisConfig` set, every buffered item is persisted under
`{Prefix}:{namespace}:{key}` before the send attempt and confirmed (deleted)
on success, so a crash between buffer and network loses nothing. See
[Configuration](configuration.md).
