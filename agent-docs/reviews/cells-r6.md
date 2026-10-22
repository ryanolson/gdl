# Review of the cells proposal, round 6

Reviewer: Codex, `gpt-6.1-sol`, reasoning `max`, the `general-review` skill, read-only, in @thread:thr_kttuv4kbr2 (2026-10-22). Target: `agent-docs/cells.md` at `8c5caaa`. Verdict: REJECT, with three major findings, all in how a follow link closes (12.2). The reviewer confirmed that the round 5 changes to eligibility, peer discovery, authentication, fail-stop, and the velo list resolve their findings. The reviewer's text is in `cells-r6-raw.txt`.

| # | Severity | Finding | Ruling | Evidence checked | Change |
|---|---|---|---|---|---|
| 1 | major | A directory expiry closes a live follow and unregisters its peers, contradicting 4.1 | valid | `cells.md` 4.1 against 12.2; `crates/gdl/src/dist/fabric.rs:74` | Expiry and withdrawal no longer close a live follow; a lost follow link keeps registrations until the leases end (12.2) |
| 2 | major | The wait assumed a longest request lifetime that gdl does not enforce | valid | `crates/gdl/src/dist/runtime.rs:46`, `crates/gdl/src/dist/hop.rs:352` (`default_deadline` only fills in a missing deadline) | No timer: each offer holds a lease on (cell, generation) until the chooser stops holding the request (12.2) |
| 3 | major | Nothing confirms when the last offer was made: instances apply membership changes asynchronously | valid | `crates/gdl/src/dist/fabric/hub.rs:544` | Each instance acknowledges the exclusion per generation once its leases reach zero; the link closes after every instance has (12.2); tests for a held delivery, an explicit long deadline, and a directory expiry (section 14) |
