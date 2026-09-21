package guardagent

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis persistence, mirroring guard-agent (Python) _buffer_redis.py:
// globally-unique "{prefix}:{namespace}:{short}" keys under the
// "agent_events" and "agent_metrics" namespaces, written on enqueue with a
// TTL, deleted on confirmation, and reloaded on Start. Every Redis failure
// is fail-open: the agent logs, counts, and keeps operating from memory.
const (
	persistNamespaceEvents  = "agent_events"
	persistNamespaceMetrics = "agent_metrics"

	// persistOpTimeout bounds one Redis round trip so a misbehaving Redis
	// can never stall the host application's request path for long.
	persistOpTimeout = 2 * time.Second

	// After this many consecutive failed writes, persistence pauses for
	// redisFailureCooldown so an unhealthy Redis cannot tax every enqueue.
	redisMaxConsecutiveFailures = 3
	redisFailureCooldown        = 30 * time.Second

	// persistScanCount is the SCAN page size used during startup reload.
	persistScanCount = 200
)

type persistence struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
	logger *log.Logger

	mu                  sync.Mutex
	consecutiveFailures int
	cooldownUntil       time.Time
	failures            int64
}

func newPersistence(cfg *RedisConfig, logger *log.Logger) (*persistence, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, err
	}
	opts.DialTimeout = persistOpTimeout
	opts.ReadTimeout = persistOpTimeout
	opts.WriteTimeout = persistOpTimeout
	return &persistence{
		client: redis.NewClient(opts),
		prefix: cfg.Prefix,
		ttl:    cfg.TTL,
		logger: logger,
	}, nil
}

// shortKey returns the globally-unique short part of a persistence key:
// "{kind}_{unix_nanos}_{8 hex chars}".
func shortKey(kind string) string {
	return fmt.Sprintf("%s_%d_%s", kind, time.Now().UnixNano(), randomHex(4))
}

func (p *persistence) fullKey(namespace, short string) string {
	return p.prefix + ":" + namespace + ":" + short
}

func shortOf(fullKey string) string {
	if i := strings.LastIndex(fullKey, ":"); i >= 0 {
		return fullKey[i+1:]
	}
	return fullKey
}

// storeAvailable reports whether write attempts should proceed. After
// redisMaxConsecutiveFailures consecutive failures writes pause for
// redisFailureCooldown.
func (p *persistence) storeAvailable() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !time.Now().Before(p.cooldownUntil)
}

// store writes value under key with the configured TTL. It returns true
// only when the record is durable. Failures are fail-open: logged,
// counted, and subject to the write cooldown.
func (p *persistence) store(key, value string) bool {
	if !p.storeAvailable() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistOpTimeout)
	defer cancel()
	err := p.client.Set(ctx, key, value, p.ttl).Err()
	if err == nil {
		p.mu.Lock()
		p.consecutiveFailures = 0
		p.mu.Unlock()
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures++
	p.consecutiveFailures++
	if p.consecutiveFailures >= redisMaxConsecutiveFailures {
		p.cooldownUntil = time.Now().Add(redisFailureCooldown)
		p.consecutiveFailures = 0
		p.logger.Printf("guardagent: redis persistence failing (%v); pausing persist writes for %s", err, redisFailureCooldown)
	} else {
		p.logger.Printf("guardagent: redis persist failed for %s: %v", key, err)
	}
	return false
}

// del deletes confirmed records. It is best effort: the TTL is the
// backstop, so failures are logged but never surfaced.
func (p *persistence) del(keys []string) {
	if len(keys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), persistOpTimeout)
	defer cancel()
	if err := p.client.Del(ctx, keys...).Err(); err != nil {
		p.logger.Printf("guardagent: redis confirm (delete) failed for %d key(s): %v", len(keys), err)
	}
}

// persistedItem is one reloaded record.
type persistedItem struct {
	Short string
	Value string
}

// loadKind scans a namespace and reads every live record, oldest short key
// first. The short key embeds unix nanos, so lexicographic order is
// chronological for the foreseeable future.
func (p *persistence) loadKind(ctx context.Context, namespace string) ([]persistedItem, error) {
	scanCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pattern := p.prefix + ":" + namespace + ":*"
	var shorts []string
	var cursor uint64
	for {
		page, next, err := p.client.Scan(scanCtx, cursor, pattern, persistScanCount).Result()
		if err != nil {
			return nil, err
		}
		shorts = append(shorts, page...)
		if next == 0 {
			break
		}
		cursor = next
	}
	sort.Strings(shorts)
	items := make([]persistedItem, 0, len(shorts))
	for _, full := range shorts {
		value, err := p.client.Get(scanCtx, full).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, err
		}
		items = append(items, persistedItem{Short: shortOf(full), Value: value})
	}
	return items, nil
}

func (p *persistence) close() error {
	return p.client.Close()
}
