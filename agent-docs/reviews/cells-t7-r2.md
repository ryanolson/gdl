# A chooser follows the cells its hub follows: review round 2

Reviewer: the same reviewer as round 1, read-only (2026-10-23). Target: `872cc29..319f731`. The reviewer accepted the four rulings of round 1; an allocation probe measured 852 bytes per event with either 1 or 1,024 instances in another cell. The workspace suite passed (863). Verdict: APPROVE-WITH-CHANGES, one major finding.

| # | Severity | Finding | Ruling | Evidence | Change |
|---|---|---|---|---|---|
| 1 | major | The chooser took "the membership no longer routes to it" as leave to unregister a peer, but the membership marks a peer left at once and unregisters it only when its own drain ends, so a peer that left the table first and the hub second lost its registration while the membership still drained it | valid | `a_later_membership_drain_keeps_its_peer` (unit and integration) recorded the chooser's unregister before the membership's deadline | the shared state keeps the peers the membership is still draining; the chooser gives such a peer up without unregistering it, and the membership unregisters it at the end of its drain (`baf1232`) |
