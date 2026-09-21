package guardagent

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Install identity. The agent reports a stable install ID in the
// X-Agent-Install-Id header. Resolution order mirrors guard-agent
// (Python) install_id.py: an explicit config override wins; otherwise the
// ID is read from (or created in) a state file, defaulting to
// ~/.guard-agent/install-id. Every filesystem failure is fail-open: the
// agent logs and falls back to a fresh in-memory UUID rather than surfacing
// an error to the host application.
func resolveInstallID(override, path string, logger *log.Logger) string {
	if override != "" {
		return override
	}
	if path == "" {
		path = defaultInstallIDPath()
	}
	if path == "" {
		return newUUID4()
	}
	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		logger.Printf("guardagent: could not read install id file %s: %v", path, err)
	}
	id := newUUID4()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		logger.Printf("guardagent: could not create install id directory %s: %v", filepath.Dir(path), err)
		return id
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		logger.Printf("guardagent: could not persist install id to %s: %v", path, err)
	}
	return id
}

func defaultInstallIDPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".guard-agent", "install-id")
}
