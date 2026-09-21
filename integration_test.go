//go:build integration

package guardagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const integrationRedisPrefix = "guard_agent_go_test"

// integrationRedisHost returns REDIS_HOST or skips the test, matching the
// sibling Go repos: REDIS_HOST=127.0.0.1 go test -tags integration ./...
func integrationRedisHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		t.Skip("REDIS_HOST not set")
	}
	return host
}

func newIntegrationRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	host := integrationRedisHost(t)
	client := redis.NewClient(&redis.Options{Addr: host + ":6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() {
		deleteTestKeys(t, client)
		_ = client.Close()
	})
	return client
}

func deleteTestKeys(t *testing.T, client *redis.Client) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	for {
		page, next, err := client.Scan(ctx, cursor, integrationRedisPrefix+":*", 100).Result()
		if err != nil {
			t.Logf("cleanup scan failed: %v", err)
			return
		}
		if len(page) > 0 {
			_ = client.Del(ctx, page...).Err()
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

func testKeys(t *testing.T, client *redis.Client) []string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	var cursor uint64
	for {
		page, next, err := client.Scan(ctx, cursor, integrationRedisPrefix+":*", 100).Result()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		keys = append(keys, page...)
		if next == 0 {
			return keys
		}
		cursor = next
	}
}

// newIntegrationAgent builds an agent wired to real Redis under the shared
// test prefix, mirroring the sibling repos' integration setup.
func newIntegrationAgent(t *testing.T, endpoint string, mutate func(*Config)) *Agent {
	t.Helper()
	host := integrationRedisHost(t)
	cfg := DefaultConfig()
	cfg.APIKey = "test-api-key-123"
	cfg.Endpoint = endpoint
	cfg.FlushInterval = time.Hour
	cfg.StatusInterval = time.Hour
	cfg.RetryAttempts = 0
	cfg.InstallIDPath = filepath.Join(t.TempDir(), "install-id")
	cfg.Redis = &RedisConfig{
		URL:    "redis://" + host + ":6379",
		Prefix: integrationRedisPrefix,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	agent, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = agent.Stop(context.Background()) })
	return agent
}

func TestIntegrationPersistsOnEnqueueWithTTL(t *testing.T) {
	client := newIntegrationRedisClient(t)
	m := newMockIngest(t)
	agent := newIntegrationAgent(t, m.URL, nil)
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		if err := agent.SendEvent(ctx, testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	keys := testKeys(t, client)
	if len(keys) != 2 {
		t.Fatalf("persisted keys got %d want 2", len(keys))
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, integrationRedisPrefix+":"+persistNamespaceEvents+":") {
			t.Fatalf("unexpected key shape %q", key)
		}
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("ttl: %v", err)
		}
		if ttl <= 0 || ttl > defaultPersistTTL {
			t.Fatalf("ttl got %s want within (0, %s]", ttl, defaultPersistTTL)
		}
	}
	if agent.Stats().RedisPersistFailures != 0 {
		t.Fatalf("healthy redis must not report persist failures: %+v", agent.Stats())
	}
}

func TestIntegrationConfirmDeletesRecords(t *testing.T) {
	client := newIntegrationRedisClient(t)
	m := newMockIngest(t)
	agent := newIntegrationAgent(t, m.URL, nil)
	ctx := context.Background()

	if err := agent.SendMetric(ctx, testMetric(1)); err != nil {
		t.Fatalf("SendMetric: %v", err)
	}
	if len(testKeys(t, client)) != 1 {
		t.Fatalf("expected one persisted metric record")
	}
	if err := agent.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(m.callsTo(metricsPath)) != 1 {
		t.Fatal("metric batch must reach the mock ingestion API")
	}
	waitUntil(t, 2*time.Second, "confirmed records must be deleted", func() bool {
		return len(testKeys(t, client)) == 0
	})
}

func TestIntegrationReloadAfterRestart(t *testing.T) {
	client := newIntegrationRedisClient(t)
	ctx := context.Background()

	// First run: the ingestion endpoint is down, so the batch fails, is
	// requeued, and stays persisted.
	first := newIntegrationAgent(t, "http://127.0.0.1:1", nil)
	for i := 1; i <= 2; i++ {
		if err := first.SendEvent(ctx, testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	if err := first.Flush(ctx); err == nil {
		t.Fatal("Flush against a down endpoint must fail")
	}
	if err := first.Stop(ctx); err == nil {
		t.Fatal("Stop with a down endpoint must report the final flush error")
	}
	if got := len(testKeys(t, client)); got != 2 {
		t.Fatalf("records after failed run got %d want 2", got)
	}

	// Second run: a fresh agent with the same Redis prefix reloads the
	// records on Start and delivers them in order.
	m := newMockIngest(t)
	second := newIntegrationAgent(t, m.URL, nil)
	if err := second.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := second.Stats().EventsPending; got != 2 {
		t.Fatalf("reloaded pending events got %d want 2", got)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 1 || len(calls[0].Events) != 2 {
		t.Fatalf("reloaded batch calls: %+v", calls)
	}
	if calls[0].Events[0].EventType != "event_1" || calls[0].Events[1].EventType != "event_2" {
		t.Fatalf("reload order got [%s %s] want [event_1 event_2]",
			calls[0].Events[0].EventType, calls[0].Events[1].EventType)
	}
	waitUntil(t, 2*time.Second, "reloaded records must be confirmed", func() bool {
		return len(testKeys(t, client)) == 0
	})
}

func TestIntegrationDropEvictsPersistedRecord(t *testing.T) {
	client := newIntegrationRedisClient(t)
	m := newMockIngest(t)
	agent := newIntegrationAgent(t, m.URL, func(c *Config) { c.BufferSize = 1 })
	ctx := context.Background()

	if err := agent.SendEvent(ctx, testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent 1: %v", err)
	}
	if err := agent.SendEvent(ctx, testEvent(2, "")); err != nil {
		t.Fatalf("SendEvent 2: %v", err)
	}
	if got := agent.Stats().EventsDropped; got != 1 {
		t.Fatalf("dropped got %d want 1", got)
	}
	waitUntil(t, 2*time.Second, "the evicted record must be deleted so it cannot resurrect", func() bool {
		return len(testKeys(t, client)) == 1
	})
	keys := testKeys(t, client)
	value, err := client.Get(ctx, keys[0]).Result()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !strings.Contains(value, "event_2") || strings.Contains(value, "event_1") {
		t.Fatalf("surviving record must be event_2, got %s", value)
	}
}

func TestIntegrationRedisFailOpen(t *testing.T) {
	integrationRedisHost(t) // keep the skip behavior consistent
	m := newMockIngest(t)
	agent := newIntegrationAgent(t, m.URL, func(c *Config) {
		c.Redis.URL = "redis://127.0.0.1:1" // closed port
	})
	ctx := context.Background()
	if err := agent.Start(ctx); err != nil {
		t.Fatalf("Start must be fail-open when redis is unreachable, got %v", err)
	}
	if err := agent.SendEvent(ctx, testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent with redis down: %v", err)
	}
	if err := agent.Flush(ctx); err != nil {
		t.Fatalf("Flush with redis down: %v", err)
	}
	if len(m.callsTo(eventsPath)) != 1 {
		t.Fatal("telemetry must still be delivered without redis")
	}
	stats := agent.Stats()
	if stats.RedisPersistFailures == 0 {
		t.Fatalf("redis failures must be counted: %+v", stats)
	}
	if !stats.DurabilityDegraded {
		t.Fatalf("durability must be reported degraded: %+v", stats)
	}
}
