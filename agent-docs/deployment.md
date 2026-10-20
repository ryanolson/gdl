# Compose deployment

The hub runs Velo's registration service with a GDL admission extension. Its input is the typed manifest and placement. Each generated segment presents its name and the contract and placement hashes compiled into that binary. Missing or mismatched registrations are rejected before they enter membership.

Compose starts the hub first and waits for its HTTP health check. It then starts all segments together. Each segment waits for one admitted peer per segment, installs its handlers, and publishes a readiness service. The frontend serves after every expected peer has announced readiness. Ordering the segments themselves would deadlock this barrier.

This implementation takes a roster snapshot at startup. It does not support replica churn or hub recovery. A lease failure ends the segment process. Replacing a segment requires restarting the stack. Request dispatch still uses the runtime's configured router policy; responses retain their stream anchors.

The build bundle snapshots non-ignored Git worktree files for local dependencies. Symlinks are omitted. The copied source retains its original absolute path inside the builder, so generated path dependencies resolve without requiring that directory on the Docker host. Runtime images contain only their executable and configuration. A segment can override the deployment's runtime base image; the hub has its own override. The Rust build image is independent.

## Validation on 2026-10-19

- Velo main: `d793db4cc4930e1e8e3a08eff23c4643f020608d`.
- Hub integration: rejected a mismatched contract, discovered two admitted peers, and completed the readiness barrier over OS-assigned ports.
- Generator unit tests: 86 passed. Model unit tests: 2 passed.
- Deployment generation tests: 2 passed. Registration identity test: 1 passed.
- Web API tests: 25 passed. Editor tests: 67 passed.
- Runtime service tests: 8 passed, including four segments and independent response paths.
- Workspace clippy passed with all targets and all features. Formatting passed.
- Browser: Graph retained 90% zoom after Segments was set to 80%. Deploy displayed four segments and downloaded a bundle with five services, per-segment Dockerfiles, the graph definition, and local sources.
- Built all four generated binaries from the extracted source snapshot. Ran them with the hub as separate native processes. All four announced ready. The chat, responses, and messages HTTP endpoints each returned the two expected NDJSON stream items through the shared Echo sink.

Native smoke-test transcript: `/tmp/gdl-native-run.log`. Native smoke-test driver: `/tmp/gdl-compose-native-check.py`. Downloaded bundle: `/Users/ryan/Downloads/composed-compose.tar.gz`. These are workstation artifacts, not repository fixtures.

Docker is not installed on this workstation. The owner will run `docker compose up --build` on another machine. No container build or execution is claimed by the native checks.
