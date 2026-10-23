# The `dir` directory backend: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-22). Target: `7ad806d..de458a1`. The reviewer accepted the three rulings of round 1, removing each fix in a `/tmp` copy to see its regression fail. Verdict: APPROVE-WITH-CHANGES, one minor finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | minor | Round 1's timestamp test published seq 2 before restoring the old time, so a poll in between could read seq 2 by its new time and pass without the inode and change-time check | valid | with that check removed and a 150 ms pause between the publish and the restore, the old test passed | the test builds the replacement beside the file with the old time and the same length, checks both, then renames it in, so no scan sees a new time; it fails 5 of 5 runs without the check and passes on the code (`3aa4658`) |
