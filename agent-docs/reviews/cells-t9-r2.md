# Refusal as an offer, and failover before acceptance: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-23). Target: `c77fed1..f17ea4a`. Verdict: APPROVE, no findings.

Checked and fine: round 1's three findings are fixed: the allocation regression reproduces `[11, 12, 16, 18]` before and `[11, 11, 16, 16]` after (single sends vary by one under load); `send_forward`'s future grows from 880 to 912 bytes and `Fabric::call`'s and `Fabric::offer`'s are 376 each, as reported, within the coordinator's ruling; the escaped marker round-trips and the earlier-build wire tests pass; the exhaustion mutant fails the new case. The workspace suite passed (902, 7 ignored), with fmt, clippy, and the runtime's clippy without `hub` and with all features.
