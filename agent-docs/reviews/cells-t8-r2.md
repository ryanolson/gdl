# A chooser chooses a cell per attempt: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-23). Target: `35e5fef..d15b219`. Verdict: APPROVE, no findings.

Checked and fine: round 1's three findings are fixed: removing the turn's preservation fails its regression while the newer-generation control passes; the early lease release fails the new lease test while the other eight pass; suppressing the selector's arrival or blocking its task now fails in 5.01 s rather than hanging. The carried-over turn after a hub restart only sets where a round robin starts, not membership or retirement. The workspace suite passed (887, 7 ignored), with fmt, clippy, and the build without `hub`. The instruction gate stays as round 1 records it: not yet verified.
