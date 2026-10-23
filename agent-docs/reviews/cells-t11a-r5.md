# A cell drains: review round 5

Reviewer: the same reviewer, read-only (2026-10-23). Target: `ecfac45..9bfd605`. Verdict: APPROVE, no findings.

Checked and fine: round 4's finding is fixed: the collect-then-scan variant fails the retained test and the one-pass table passes. A `retire` test (`a_retired_input_releases_what_it_buffered_before_anyone_reads_it`) that failed once in the implementation's full run fails at this change's base too under load (live bytes about 527,700 against a 65,536-byte limit) and passed 100 isolated runs at both: it predates this change, and its cause is not yet known. The runtime's unit tests, `draining`, `boundary`, `failover`, fmt, and clippy with and without default features pass.
