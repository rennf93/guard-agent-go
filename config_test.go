package guardagent

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testInstallIDPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "install-id")
}

func TestNewFillsDefaults(t *testing.T) {
	agent, err := New(Config{APIKey: "test-api-key", InstallIDPath: testInstallIDPath(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = agent.Stop(context.Background()) }()
	if agent.cfg.BufferSize != 100 {
		t.Fatalf("BufferSize got %d want 100", agent.cfg.BufferSize)
	}
	if agent.cfg.FlushInterval != 30*time.Second {
		t.Fatalf("FlushInterval got %s want 30s", agent.cfg.FlushInterval)
	}
	if agent.cfg.StatusInterval != 300*time.Second {
		t.Fatalf("StatusInterval got %s want 300s", agent.cfg.StatusInterval)
	}
	if agent.cfg.RetryAttempts != 0 {
		t.Fatalf("RetryAttempts got %d want 0 (zero-honest: DefaultConfig sets 3)", agent.cfg.RetryAttempts)
	}
	if agent.cfg.Timeout != 30*time.Second {
		t.Fatalf("Timeout got %s want 30s", agent.cfg.Timeout)
	}
	if agent.cfg.BackoffFactor != 1.0 {
		t.Fatalf("BackoffFactor got %v want 1.0", agent.cfg.BackoffFactor)
	}
	if agent.cfg.HighWatermarkRatio != 0.8 {
		t.Fatalf("HighWatermarkRatio got %v want 0.8", agent.cfg.HighWatermarkRatio)
	}
	if agent.cfg.MaxConcurrentFlushes != 1 {
		t.Fatalf("MaxConcurrentFlushes got %d want 1", agent.cfg.MaxConcurrentFlushes)
	}
	if agent.cfg.Overflow != OverflowDrop {
		t.Fatalf("Overflow got %q want drop", agent.cfg.Overflow)
	}
	if agent.cfg.CompressionThreshold != 0 {
		t.Fatalf("CompressionThreshold got %d want 0 (zero-honest: DefaultConfig sets 1024)", agent.cfg.CompressionThreshold)
	}
	if agent.cfg.Endpoint != DefaultEndpoint {
		t.Fatalf("Endpoint got %q want %q", agent.cfg.Endpoint, DefaultEndpoint)
	}
	if agent.installID == "" {
		t.Fatal("install id must be resolved")
	}
}

func TestDefaultConfigEnablesFeatures(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.EnableEvents || !cfg.EnableMetrics || !cfg.CompressionEnabled {
		t.Fatal("DefaultConfig must enable events, metrics, and compression")
	}
	if cfg.RetryAttempts != 3 {
		t.Fatalf("DefaultConfig RetryAttempts got %d want 3", cfg.RetryAttempts)
	}
	if cfg.CompressionThreshold != 1024 {
		t.Fatalf("DefaultConfig CompressionThreshold got %d want 1024", cfg.CompressionThreshold)
	}
}

func TestConfigValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		problem string
	}{
		{"missing api key", Config{InstallIDPath: "x"}, "api key"},
		{"short api key", Config{APIKey: "short", InstallIDPath: "x"}, "at least 10"},
		{"bad scheme", Config{APIKey: "test-api-key", Endpoint: "ftp://example.com", InstallIDPath: "x"}, "http or https"},
		{"not a url", Config{APIKey: "test-api-key", Endpoint: "not a url", InstallIDPath: "x"}, "valid URL"},
		{"negative buffer", Config{APIKey: "test-api-key", BufferSize: -1, InstallIDPath: "x"}, "buffer size"},
		{"negative flush", Config{APIKey: "test-api-key", FlushInterval: -time.Second, InstallIDPath: "x"}, "flush interval"},
		{"small status interval", Config{APIKey: "test-api-key", StatusInterval: 30 * time.Second, InstallIDPath: "x"}, "at least 60s"},
		{"watermark above one", Config{APIKey: "test-api-key", HighWatermarkRatio: 1.5, InstallIDPath: "x"}, "watermark"},
		{"bad overflow", Config{APIKey: "test-api-key", Overflow: OverflowPolicy("explode"), InstallIDPath: "x"}, "overflow"},
		{"negative retry", Config{APIKey: "test-api-key", RetryAttempts: -1, InstallIDPath: "x"}, "retry attempts"},
		{"negative timeout", Config{APIKey: "test-api-key", Timeout: -time.Second, InstallIDPath: "x"}, "timeout"},
		{"negative backoff", Config{APIKey: "test-api-key", BackoffFactor: -1, InstallIDPath: "x"}, "backoff factor"},
		{"negative compression threshold", Config{APIKey: "test-api-key", CompressionThreshold: -1, InstallIDPath: "x"}, "compression threshold"},
		{"redis without url", Config{APIKey: "test-api-key", Redis: &RedisConfig{}, InstallIDPath: "x"}, "redis url"},
		{"negative redis ttl", Config{APIKey: "test-api-key", Redis: &RedisConfig{URL: "redis://localhost:6379", TTL: -time.Second}, InstallIDPath: "x"}, "redis ttl"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			if err == nil {
				t.Fatal("expected New to fail")
			}
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("expected ConfigError, got %T: %v", err, err)
			}
			if !strings.Contains(cfgErr.Error(), tc.problem) {
				t.Fatalf("expected problem %q in %q", tc.problem, cfgErr.Error())
			}
		})
	}
}

func TestConfigErrorAggregatesProblems(t *testing.T) {
	_, err := New(Config{APIKey: "x", StatusInterval: time.Second})
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected ConfigError, got %v", err)
	}
	if len(cfgErr.Problems) < 2 {
		t.Fatalf("expected aggregated problems, got %v", cfgErr.Problems)
	}
}

func TestEndpointNormalization(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	agent, err := New(Config{
		APIKey:        "test-api-key",
		Endpoint:      "https://example.com/api/v1/",
		InstallIDPath: testInstallIDPath(t),
	}, WithLogger(logger))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = agent.Stop(context.Background()) }()
	if agent.cfg.Endpoint != "https://example.com" {
		t.Fatalf("Endpoint got %q want https://example.com", agent.cfg.Endpoint)
	}
	if !strings.Contains(buf.String(), "/api/v1") {
		t.Fatalf("expected /api/v1 warning in log, got %q", buf.String())
	}
}

func TestWithLoggerAndClientIgnoreNil(t *testing.T) {
	agent, err := New(Config{APIKey: "test-api-key", InstallIDPath: testInstallIDPath(t)}, WithLogger(nil), WithHTTPClient(nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = agent.Stop(context.Background()) }()
	if agent.logger == nil {
		t.Fatal("logger must fall back to log.Default()")
	}
}
