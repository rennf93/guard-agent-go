package guardagent

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveInstallIDOverrideWins(t *testing.T) {
	got := resolveInstallID("override-id", filepath.Join(t.TempDir(), "install-id"), log.New(&bytes.Buffer{}, "", 0))
	if got != "override-id" {
		t.Fatalf("got %q want override-id", got)
	}
}

func TestResolveInstallIDPersistsAndReuses(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	path := filepath.Join(t.TempDir(), "install-id")

	first := resolveInstallID("", path, logger)
	if !uuid4Pattern.MatchString(first) {
		t.Fatalf("first id %q is not a uuid", first)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("install id file not persisted: %v", err)
	}
	if strings.TrimSpace(string(data)) != first {
		t.Fatalf("file contents %q want %q", string(data), first)
	}
	second := resolveInstallID("", path, logger)
	if second != first {
		t.Fatalf("second id %q must reuse the persisted %q", second, first)
	}
	if buf.Len() != 0 {
		t.Fatalf("unexpected warnings: %q", buf.String())
	}
}

func TestResolveInstallIDEmptyFileRegenerates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install-id")
	if err := os.WriteFile(path, []byte("\n"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	got := resolveInstallID("", path, log.New(&bytes.Buffer{}, "", 0))
	if !uuid4Pattern.MatchString(got) {
		t.Fatalf("expected fresh uuid, got %q", got)
	}
}

func TestResolveInstallIDFailOpen(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	// A file occupying the parent-directory slot makes MkdirAll fail.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	got := resolveInstallID("", filepath.Join(blocker, "nested", "install-id"), logger)
	if !uuid4Pattern.MatchString(got) {
		t.Fatalf("fail-open must still return a uuid, got %q", got)
	}
	if !strings.Contains(buf.String(), "could not create install id directory") {
		t.Fatalf("expected a warning log, got %q", buf.String())
	}
}
