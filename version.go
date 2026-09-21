// Package guardagent implements the Go telemetry agent for the Guard
// ecosystem. It buffers security events, metrics, and agent status locally
// and ships them to the Guard Core App ingestion API with at-least-once
// delivery guarantees: nothing acknowledged is lost, nothing
// unacknowledged is forgotten.
//
// The agent is a library, not a process: it never panics out of an exported
// method, never blocks the host beyond the documented overflow policy, and
// treats every telemetry failure as a log line plus a counter bump.
package guardagent

// Version is the semantic version of the guard-agent-go module. It is
// reported to the ingestion API as agent_version and in the User-Agent
// header. Releases are published as v-prefixed git tags.
const Version = "0.1.0"
