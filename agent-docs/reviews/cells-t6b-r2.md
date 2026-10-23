# Following home cells, the relay, and `requires`: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-23). Target: `55ba024..f666fde`. Verdict: APPROVE, no findings.

Checked and fine: the three findings of round 1 are fixed: each regression fails with its fix removed in a `/tmp` copy (counts 0 and 1; the absent cell still followed at generation 234; a snapshot after shutdown) and passes with it. Follows use (class, cell) throughout installation, updates, removal, and relay events; the rewatch reset keeps record versions, tombstones, and live follows. fmt, clippy, and the 67 hub tests (one ignored, as before) pass.
