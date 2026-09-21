package guardagent

import (
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultEndpoint is the Guard Core App ingestion base URL used when
// Config.Endpoint is empty.
const DefaultEndpoint = "https://api.guard-core.com"

// Defaults applied by New when the corresponding Config field is zero.
const (
	defaultBufferSize       = 100
	defaultFlushInterval    = 30 * time.Second
	defaultStatusInterval   = 300 * time.Second
	defaultTimeout          = 30 * time.Second
	defaultBackoffFactor    = 1.0
	defaultHighWatermark    = 0.8
	defaultMaxConcurrent    = 1
	defaultRetryAttempts    = 3
	defaultCompressThresh   = 1024
	defaultRedisPrefix      = "guard:agent"
	defaultPersistTTL       = 3600 * time.Second
	minAPIKeyLength         = 10
	minStatusInterval       = 60 * time.Second
	apiVersionPathSuffix    = "/api/v1"
	apiVersionSuffixWarning = "endpoint ends with /api/v1; the agent appends API paths itself, so the suffix was removed"
)

// OverflowPolicy selects the behavior of SendEvent and SendMetric when the
// per-kind buffer is full.
type OverflowPolicy string

const (
	// OverflowDrop (default) evicts the oldest buffered item of the same
	// kind, confirms (deletes) its persisted record, and counts a drop.
	OverflowDrop OverflowPolicy = "drop"

	// OverflowBlock waits for flush to free space, polling every 500ms and
	// returning ctx.Err() when the context ends. A requeued batch always
	// keeps its slots: durability wins over a new writer.
	OverflowBlock OverflowPolicy = "block"

	// OverflowRaise makes SendEvent and SendMetric return a
	// *BufferFullError (errors.Is ErrBufferFull) without buffering.
	OverflowRaise OverflowPolicy = "raise"
)

// RedisConfig enables optional Redis durability for buffered telemetry.
// Records are written on enqueue with a TTL, deleted on confirmation, and
// reloaded on Start, so a crash between enqueue and confirmation does not
// lose telemetry. Every Redis failure is fail-open: the agent logs and
// counts the failure and keeps operating from memory.
type RedisConfig struct {
	// URL is a redis:// or rediss:// connection string, as accepted by
	// redis.ParseURL.
	URL string
	// Prefix namespaces every key; default "guard:agent". Keys are
	// "{Prefix}:{namespace}:{short-key}" with namespaces "agent_events"
	// and "agent_metrics".
	Prefix string
	// TTL bounds record lifetime; default 1 hour.
	TTL time.Duration
}

// Config configures an Agent. Start from DefaultConfig and override what
// you need. Booleans are explicit: the zero value of EnableEvents,
// EnableMetrics, and CompressionEnabled is false, so a hand-built Config
// that omits them disables that feature; DefaultConfig turns them on.
type Config struct {
	// APIKey authenticates against the ingestion API (X-API-Key). Required,
	// at least 10 characters.
	APIKey string
	// Endpoint is the ingestion base URL, default DefaultEndpoint. A
	// trailing slash and a trailing /api/v1 suffix are removed.
	Endpoint string
	// ProjectID is sent as X-Project-Id when non-empty.
	ProjectID string

	// BufferSize caps each per-kind queue; default 100.
	BufferSize int
	// FlushInterval is the periodic flush cadence and the base of the
	// per-kind failure backoff; default 30s.
	FlushInterval time.Duration
	// StatusInterval is the status reporting cadence, minimum 60s,
	// default 300s.
	StatusInterval time.Duration
	// HighWatermarkRatio triggers an early flush once the combined buffer
	// occupancy reaches ratio * BufferSize; default 0.8.
	HighWatermarkRatio float64
	// MaxConcurrentFlushes bounds in-flight flush cycles; default 1.
	MaxConcurrentFlushes int
	// Overflow selects the full-buffer policy; default OverflowDrop.
	Overflow OverflowPolicy

	// EnableEvents enables SendEvent; default true in DefaultConfig.
	EnableEvents bool
	// EnableMetrics enables SendMetric; default true in DefaultConfig.
	EnableMetrics bool

	// RetryAttempts is the number of retries after a failed attempt.
	// Zero disables retries (zero-honest field); DefaultConfig sets 3.
	RetryAttempts int
	// Timeout bounds one HTTP request; default 30s.
	Timeout time.Duration
	// BackoffFactor is the base retry delay in seconds; default 1.0.
	BackoffFactor float64

	// CompressionEnabled gzips bodies at or above CompressionThreshold;
	// default true in DefaultConfig.
	CompressionEnabled bool
	// CompressionThreshold is the gzip cutoff in bytes. Zero compresses
	// every body (zero-honest field); DefaultConfig sets 1024.
	CompressionThreshold int

	// SigningSecret signs payloads. When empty, no signature header is
	// sent. The HMAC-SHA256 signature covers the uncompressed JSON body.
	SigningSecret string

	// InstallID overrides the persisted install id.
	InstallID string
	// InstallIDPath overrides the install id state file, default
	// ~/.guard-agent/install-id.
	InstallIDPath string

	// Redis enables optional persistence; nil disables it.
	Redis *RedisConfig

	// GuardVersion and GuardCoreVersion are reported to the ingestion API
	// as guard_version and guard_core_version.
	GuardVersion     string
	GuardCoreVersion string
}

// DefaultConfig returns a Config with the same defaults as guard-agent
// (Python) AgentConfig: 100-item buffers, 30s flush, 300s status, 0.8
// watermark, drop overflow, 3 retries, 30s timeout, gzip at 1024 bytes,
// events and metrics enabled.
func DefaultConfig() Config {
	return Config{
		Endpoint:             DefaultEndpoint,
		BufferSize:           defaultBufferSize,
		FlushInterval:        defaultFlushInterval,
		StatusInterval:       defaultStatusInterval,
		HighWatermarkRatio:   defaultHighWatermark,
		MaxConcurrentFlushes: defaultMaxConcurrent,
		Overflow:             OverflowDrop,
		EnableEvents:         true,
		EnableMetrics:        true,
		RetryAttempts:        defaultRetryAttempts,
		Timeout:              defaultTimeout,
		BackoffFactor:        defaultBackoffFactor,
		CompressionEnabled:   true,
		CompressionThreshold: defaultCompressThresh,
	}
}

type options struct {
	logger     *log.Logger
	httpClient *http.Client
}

// Option customizes an Agent at construction time.
type Option func(*options)

// WithLogger replaces the default logger (log.Default()). Nil is ignored.
func WithLogger(logger *log.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithHTTPClient replaces the default HTTP client. Nil is ignored. The
// client should not follow redirects, matching the reference agents.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) {
		if client != nil {
			o.httpClient = client
		}
	}
}

// normalize fills defaults and validates cfg, returning a ConfigError with
// every problem found. It mirrors guard-agent (Python) models.py field
// validators plus utils.validate_config.
func normalize(cfg Config) (Config, []string, error) {
	var problems []string
	var warnings []string

	if cfg.APIKey == "" {
		problems = append(problems, "api key is required")
	} else if len(cfg.APIKey) < minAPIKeyLength {
		problems = append(problems, "api key must be at least 10 characters")
	}

	endpoint, endpointWarnings, err := normalizeEndpoint(cfg.Endpoint)
	if err != nil {
		problems = append(problems, err.Error())
	} else {
		cfg.Endpoint = endpoint
		warnings = append(warnings, endpointWarnings...)
	}

	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaultBufferSize
	} else if cfg.BufferSize < 0 {
		problems = append(problems, "buffer size must be greater than 0")
	}

	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = defaultFlushInterval
	} else if cfg.FlushInterval < 0 {
		problems = append(problems, "flush interval must be greater than 0")
	}

	if cfg.StatusInterval == 0 {
		cfg.StatusInterval = defaultStatusInterval
	} else if cfg.StatusInterval < minStatusInterval {
		problems = append(problems, "status interval must be at least 60s")
	}

	if cfg.HighWatermarkRatio == 0 {
		cfg.HighWatermarkRatio = defaultHighWatermark
	} else if cfg.HighWatermarkRatio < 0 || cfg.HighWatermarkRatio > 1 {
		problems = append(problems, "high watermark ratio must be in (0, 1]")
	}

	if cfg.MaxConcurrentFlushes == 0 {
		cfg.MaxConcurrentFlushes = defaultMaxConcurrent
	} else if cfg.MaxConcurrentFlushes < 0 {
		problems = append(problems, "max concurrent flushes must be at least 1")
	}

	if cfg.Overflow == "" {
		cfg.Overflow = OverflowDrop
	} else {
		switch cfg.Overflow {
		case OverflowDrop, OverflowBlock, OverflowRaise:
		default:
			problems = append(problems, `overflow policy must be one of "drop", "block", "raise"`)
		}
	}

	// RetryAttempts is zero-honest: 0 disables retries, so it is NOT
	// default-filled. DefaultConfig sets 3.
	if cfg.RetryAttempts < 0 {
		problems = append(problems, "retry attempts must be 0 or greater")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	} else if cfg.Timeout < 0 {
		problems = append(problems, "timeout must be greater than 0")
	}

	if cfg.BackoffFactor == 0 {
		cfg.BackoffFactor = defaultBackoffFactor
	} else if cfg.BackoffFactor < 0 {
		problems = append(problems, "backoff factor must be greater than 0")
	}

	// CompressionThreshold is zero-honest: 0 compresses every body, so it
	// is NOT default-filled. DefaultConfig sets 1024.
	if cfg.CompressionThreshold < 0 {
		problems = append(problems, "compression threshold must be 0 or greater")
	}

	if cfg.InstallIDPath == "" {
		cfg.InstallIDPath = defaultInstallIDPath()
	}

	if cfg.Redis != nil {
		if cfg.Redis.URL == "" {
			problems = append(problems, "redis url is required when redis persistence is enabled")
		} else if _, err := url.Parse(cfg.Redis.URL); err != nil {
			problems = append(problems, "redis url must be a redis:// or rediss:// connection string")
		}
		if cfg.Redis.Prefix == "" {
			cfg.Redis.Prefix = defaultRedisPrefix
		}
		if cfg.Redis.TTL == 0 {
			cfg.Redis.TTL = defaultPersistTTL
		} else if cfg.Redis.TTL < 0 {
			problems = append(problems, "redis ttl must be greater than 0")
		}
	}

	if len(problems) > 0 {
		return cfg, warnings, &ConfigError{Problems: problems}
	}
	return cfg, warnings, nil
}

// normalizeEndpoint validates and canonicalizes the base URL the same way
// guard-agent (Python) models.py endpoint validator does: http/https only,
// trailing slashes removed, and a trailing /api/v1 removed with a warning
// because the agent appends API paths itself.
func normalizeEndpoint(raw string) (string, []string, error) {
	if raw == "" {
		return DefaultEndpoint, nil, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return raw, nil, &ConfigError{Problems: []string{"endpoint must be a valid URL"}}
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return raw, nil, &ConfigError{Problems: []string{"endpoint scheme must be http or https"}}
	}
	var warnings []string
	trimmed := strings.TrimRight(raw, "/")
	if strings.HasSuffix(trimmed, apiVersionPathSuffix) {
		trimmed = strings.TrimSuffix(trimmed, apiVersionPathSuffix)
		trimmed = strings.TrimRight(trimmed, "/")
		warnings = append(warnings, apiVersionSuffixWarning)
	}
	return trimmed, warnings, nil
}
