# The split, review round 5

Reviewer: one fresh Fable 5.1 worker, read-only, on the whole change. Subject: `2278f3e..6ded81f`. Verdict: APPROVE for the split.

The reviewer checked the round 4 fix with mutations in a scratch copy. With velo at another revision in the root manifest, the build compiled, and the test's velo assertion failed. With the root lock edited to another revision, the offline build used the copied lock, not the tip of cargo's git database. The record of round 4 matched the code.

It found two intermittent failures that predate the split. Both reproduce only when the whole `selectors` target runs under load: 40 `yes` processes on 20 cores, with the test binary run 80 times. Neither failed in 33 runs on an idle host or under lighter load.

- **C1, valid (the test).** `the_outer_end_holds_a_lease_while_its_response_is_open` required both of a hedging operator's attempts to choose. Under load, the loser can be dropped at the winner's first item before it reaches the selector, so 3 choices were counted, not 4. It failed 9 of 80 times on `6ded81f`, and 2 of 80 on `2278f3e`. The test now accepts 3 or 4 choices. It still requires every lease taken to be given back, which is what the test is about.
- **S1 of `m23-r1`, valid (the runtime).** `a_request_cancelled_while_its_selector_chose_is_not_handed_on` failed 7 of 80 times: a request cancelled inside its selector reached the engine. `m23-r1` had ruled this invalid from 20 passes on an idle host; that record has an addendum. `send_forward` now checks the context after the selector returns. A cancelled request then fails before it is encoded, and the choice's lease is dropped.

## Validation

Under the same load, with both changes, the `selectors` target failed 0 of 80 runs. Before them, it failed 17 of 80 (C1 10, S1 7). `cargo fmt --check` and `cargo clippy --workspace --all-targets -D warnings` pass. The workspace suite passes 651 tests, with 0 failures.
