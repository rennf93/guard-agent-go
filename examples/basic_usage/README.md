# basic_usage

Minimal guard-agent-go wiring: the guard-core-go engine's `OnBlock` hook
builds a `SecurityEvent` per blocked request and the agent buffers and ships
it to the Guard Core App ingestion API.

## Run

```bash
export GUARD_AGENT_API_KEY="your-api-key"
export GUARD_AGENT_PROJECT_ID="your-project-id"
export GUARD_AGENT_SIGNING_SECRET="your-signing-secret"   # optional, enables HMAC signing
export GUARD_AGENT_REDIS_URL="redis://localhost:6379/0"   # optional, enables persistence

go run ./examples/basic_usage
```

The example stays a wiring demonstration: it constructs the config, starts
the agent, attaches the `OnBlock` hook, and performs a final flush on
SIGINT/SIGTERM. For a full guarded HTTP service, see the adapter repos
(nethttp-guard, gin-guard, echo-guard, fiber-guard) and their advanced
examples, which combine the engine, an adapter, and this same agent wiring.

## Notes

- The direct agent API (`SendEvent`, `SendMetric`, `Status`, `Stats`) is for
  custom events or standalone deployments; most guard-core-go deployments
  only need the hook shown here.
- `guardcore.DefaultSecurityConfig()` returns a config with `OnBlock` unset;
  the example assigns it before engine construction in real use. See the
  engine's `SecurityConfig` for the payload fields the hook receives
  (`client_ip`, `path`, `method`, `check_name`, `reason`).
