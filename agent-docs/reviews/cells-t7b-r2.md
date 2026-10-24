# A watch ends on an event too large or a snapshot that never completes: review round 2

Reviewer: the same reviewer, under the same rules (2026-10-24). Target: `9b1c3e7..e72c936`. Verdict: APPROVE, no findings. Findings 1 to 3 are closed: removing the deadline check, or the unfinished event's count, fails its kept test, and the missing-entry probe catches a disabled deadline while the healthy 64-instance control passes. One timer per watch, and nothing on a request's path. Fmt and clippy passed, and so did 203 library tests, 56 hub integration tests, and 42 runtime integration tests. The instruction gate stays as earlier records say: not yet verified.

Scores: correctness 9, tests 9, fit 10, code quality 9.
