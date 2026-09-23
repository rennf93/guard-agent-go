Release Notes
=============

___

v3.0.2 (2026-09-24)
-------------------

First tagged release: parity with guard-agent 3.0.2 and the /v3 module path (v3.0.2)
------------------------------------------------------------------------------------

### Breaking Changes

- **Import paths now end in `/v3`.** The module path is `github.com/rennf93/guard-agent-go/v3` and the release tag is `v3.0.2`; Go modules reject a `v3+` tag unless the module path carries the `/v3` suffix, so the migration is mandatory for this release to be fetchable. Update every import from `github.com/rennf93/guard-agent-go` to `github.com/rennf93/guard-agent-go/v3` (package name stays `guardagent`). Installation is now `go get github.com/rennf93/guard-agent-go/v3@v3.0.2`.

### Added

- **First tagged release of the Go agent, at parity with the reference guard-agent 3.0.2 (Python).** The port covers the full agent surface: event and metric buffering with at-least-once delivery, the overflow policies, Redis persistence, circuit breaking, and the batch transport.
- **The payload-signature contract matches the server.** `X-Payload-Signature` is an HMAC over the uncompressed body: the agent signs the body before any compression is applied, and the server verifies the signature after decompression. Compressed batches therefore verify correctly on both the encrypted and unencrypted POST paths.
- **An mkdocs documentation site** under `docs/`, covering configuration, the buffering and overflow model, the transport and signing contract, and Redis persistence.
- **A `basic_usage` wiring example** (`examples/basic_usage`) that wires the agent into a guard-core-go engine through the engine's `OnBlock` telemetry seam.

### Changed

- **`version.go` is bumped to 3.0.2** so the reported `agent_version` and the User-Agent match the release tag, and `make bump-version` (via `.github/scripts/bump_version.py`) now updates both `version.go` and this changelog.
- **Makefile harmonized with the guard family.** `install`, `test` (unit plus `-tags integration` with `REDIS_HOST` in docker), `lint` (`gofmt` check plus `go vet`), `bump-version` and `clean` match the conventions used across the Python guard repos.

___
