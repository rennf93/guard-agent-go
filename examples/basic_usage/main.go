// Command basic_usage wires guard-agent-go into a guard-core-go engine the
// way a production service would: the engine's OnBlock hook builds a
// SecurityEvent per blocked request, the agent buffers and ships it to the
// Guard Core App ingestion API, and shutdown performs a final flush.
//
// Set GUARD_AGENT_API_KEY (required), GUARD_AGENT_PROJECT_ID, and
// GUARD_AGENT_SIGNING_SECRET before running. Optional persistence:
// GUARD_AGENT_REDIS_URL.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	guardagent "github.com/rennf93/guard-agent-go/v3"
	guardcore "github.com/rennf93/guard-core-go/guardcore"
)

func main() {
	logger := log.New(os.Stderr, "guard-agent-go-example ", log.LstdFlags)

	agent, err := guardagent.New(guardagent.Config{
		APIKey:           os.Getenv("GUARD_AGENT_API_KEY"),
		ProjectID:        os.Getenv("GUARD_AGENT_PROJECT_ID"),
		SigningSecret:    os.Getenv("GUARD_AGENT_SIGNING_SECRET"),
		GuardVersion:     "example",
		GuardCoreVersion: "v0.1.0",
	})
	if err != nil {
		logger.Fatalf("agent: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.Start(ctx); err != nil {
		logger.Fatalf("start: %v", err)
	}

	// Engine wiring: guard-core-go exposes OnBlock as its telemetry seam.
	// The engine hands the hook the Request and a verdict payload map; only
	// fields the payload carries are forwarded to the event. Assign OnBlock
	// before guardcore.NewEngine so every blocked request is reported.
	cfg := guardcore.DefaultSecurityConfig()
	cfg.OnBlock = func(req guardcore.Request, payload map[string]any) {
		ev := guardagent.SecurityEvent{
			Timestamp:   time.Now().UTC(),
			EventType:   "suspicious_request",
			IPAddress:   stringField(payload, "client_ip"),
			Endpoint:    stringField(payload, "path"),
			Method:      stringField(payload, "method"),
			ActionTaken: "BLOCKED",
			Reason:      stringField(payload, "reason"),
		}
		if err := agent.SendEvent(ctx, ev); err != nil {
			logger.Printf("send event: %v", err)
		}
	}

	// Finish engine setup with the hook attached, as a real service would.
	engine, err := guardcore.NewEngine(cfg)
	if err != nil {
		logger.Fatalf("engine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// A real service would install an adapter middleware (nethttp-guard,
	// gin-guard, echo-guard, fiber-guard) over this engine and serve
	// traffic; this example only demonstrates the agent wiring, so it
	// reports health and idles until interrupted.
	logger.Printf("agent healthy: %v (stats: %+v)", agent.Healthy(), agent.Stats())
	<-ctx.Done()

	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := agent.Stop(shutdown); err != nil {
		logger.Printf("stop: %v", err)
	}
}

func stringField(payload map[string]any, key string) string {
	if v, ok := payload[key].(string); ok {
		return v
	}
	return ""
}
