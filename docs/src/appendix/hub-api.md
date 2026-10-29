# Pool hub routes and watch events

A pool's hub serves the `gdl-pool` extension's routes under `/v1/ext/gdl-pool`. The public listener is port 1337 and the control listener is port 8337. [Classes and pools](../concepts/pools.md) and [Following and choosing pools](../concepts/pool-routing.md) describe what each route does. Field-level detail is the rustdoc of `gdl_watch` (wire types, routes, client) and `gdl_hub` (`PoolReadiness`, `PoolTiming`, `PoolHealth`). A hub process reads its timing from the `[pool]` keys in the table below, as `GDL_POOL__*` variables ([Segment settings](../operations/configuration.md)).

## Routes

| Method and path | Listener | Credential | Body | Answers |
|---|---|---|---|---|
| `GET /health` | public | none | none | 200 when the pool is healthy, 503 otherwise, both with a `PoolHealth` body |
| `GET /watch` | public | none | optional query `?class=&pool=&run=&generation=` (`gdl_watch::Follower`) | the pool's watch as server-sent events (`PoolWatchEvent`) |
| `GET /follows` | public | none | optional query `?instance=` (`gdl_watch::RelayWatcher`), the chooser that watches | the relay as server-sent events (`FollowEvent`) |
| `GET /follows/{class}/{pool}` | public | none | none | one pool's follows: its closing generations, then its current one, then `synced`, then each relay event that names the pool |
| `POST /release` | public | none | `Follower` | 204; the watch no longer counts as a follower |
| `POST /avoided` | public | none | `Avoided` | 204; counted only from a watch that counted when the instance retired |
| `POST /report` | public | none | `Report`: a child's record or withdrawal | 200 with a `ReportReply` (`interval_ms`, when the parent expects the next report); 400 with a `ReportRefusal` for a malformed one; 409 with a `ReportRefusal` for one the parent refuses. Served only by a hub under the `relay` backend, whether or not its pool is healthy |
| `GET /report/{class}/{pool}` | public | none | none | 200 with the `PoolRecord` the parent holds for that child, expired or not; 404 when it holds none. Served only by a hub under the `relay` backend. A restarted child's hub reads it before its first report |
| `POST /drain` | control | none | none | 202 with the health body; 409 for a pool whose class no hop enters |
| `POST /presence` | control | none | `SetPresence`: `{"presence": "serving"}` or `{"presence": "scaled_to_zero"}` | 204 once the directory has stored a record written after the request with that presence; 503 when it has not within 5 s, or a later request replaced it |
| `POST /properties/revoke` | control | none | a property's kind and key: `{"kind": "shard", "key": {"name": "a"}}`, the key's fields in any order | 204 once the directory has stored a record that leaves the property out; 400 for a malformed property (a kind that is not a settings key, or a key that is not an object); 503 when it has not within 5 s, or a later request lifted it |
| `POST /properties/lift` | control | none | a property's kind and key | 204 once the directory has stored a record that no longer revokes the property; 400 and 503 as for `revoke` |
| `POST /children/{class}/{pool}/withdraw` | control | none | none | 204 once the directory has stored the child's withdrawal, at an epoch past its last record; 404 when the hub holds no record of a child of that name and class; 409 when the directory refuses it; 503 when the directory cannot be read or written |
| `POST /children/{class}/{pool}/scale-to-zero` | control | none | none | as `withdraw`, for the child's last record written again with `scaled_to_zero` |
| `POST /instances/{id}/load` | control | the instance's | `LoadReport`, with the instance's `properties`, one list for each kind its nodes set | 204; 409 unless the instance is a usable entry instance, and before the hub has read its pool's last record. A report that carries a `draining` property is answered once a record that carries it is stored, or 503 after half a summary period |
| `POST /instances/{id}/chooser` | control | the instance's | none | 204; 409 unless the instance is of a segment that chooses pools |
| `POST /instances/{id}/unoffered` | control | the instance's | `Unoffered` | 204, whether or not it answers the current revision; 409 from an instance that is not a chooser |
| `POST /instances/{id}/avoids` | control | the instance's | `Avoids` | 204; 409 from an instance that is not a chooser |

- Refusals on the credentialed routes are 409, not 401 or 404, because velo-hub's client drops its registration on those.
- `release` and `avoided` are not authenticated. Nothing checks the names in a `Follower`, or the chooser a relay watch names.
- The hub waits for a chooser's acknowledgements while it has a relay watch open in its name, and for `[pool] lost_chooser_ms` after its last one closed or after it declared.
- `report` carries no credential either. The parent writes only what it verifies: a record of the right shape whose `home` is the parent's pool, whose contract and placement hashes are the parent's, and that is newer than what it holds. A withdrawal is written only over a record the parent holds. Any other withdrawal is answered 200 and writes nothing.
- A watch that names no follower counts for nothing in the drain and retirement waits. A watch that closes without a release still counts. Once the pool's lease has passed since it closed, it no longer shows as a follower, but it never counts as a release.
- A watch that arrives before the hub first stores the pool's record waits up to 5 s, then gets 503. Every watch ends when the hub stops, or once a drained pool's record is withdrawn. A watch after that gets 503.
- `GraphAdmission` serves `GET /v1/ext/gdl/members`, whose `PeerInfo` values the watch events carry.

### Keyed records

The keyed record service serves one route under `/v1/ext/gdl-keyed`. The wire types are `gdl_watch`'s (`KeyedAsk`, `KeyedOp`, `KeyedReply`, `KeyedAnswer`).

| Method and path | Listener | Credential | Body | Answers |
|---|---|---|---|---|
| `POST /instances/{id}/keyed` | control | the instance's | `KeyedAsk`: a list of `get`, `set`, and `check` operations, each tagged by `op`; keys and values as base64 text | 200 with a `KeyedReply`: one answer for each operation, in order, each tagged by `answer`, and `eviction_age_ms`, the age of the table's least recently used record, or `null` while it is empty. A refused write is an answer, not an error |

A request without the credential of a registered instance is refused before any operation runs. The credential is checked when the headers arrive, so a write whose body arrives after its instance left still applies: a keyed record belongs to no registration, and versions order late writes as they order any other. A request of no operations reads the eviction age and touches nothing.

## Watch events

Each server-sent event's data is one JSON value tagged by `event`.

| Watch | Snapshot, in order | Changes |
|---|---|---|
| Pool's watch (`PoolWatchEvent`) | `pool` (name, class, epoch, state, load, and `properties`, the pool's property entries and their version); `state` with `unfollowed: true` if already sent; `retired`; `ready` (segment, `PeerInfo`); `synced` | `state`; `load` once a period while there is a summary, and once when there is none; `properties` when the list changes; `ready`; `retired`; `retired` again with `acknowledged: true`; `left`; `state` again with `unfollowed: true` |
| Relay (`FollowEvent`) | for each follow: `follow` (class, pool, generation, region, state, load, `load_age_ms`, how long ago the hub received the load, and the current follow's `listing`), `offering` if not offered, `retired`, `exposed`; then `listed` for each listed pool with no follow; then one `synced`, with `listed: true` once the hub has read the directory's first snapshot of every class it sends to | `state`, `load`, `offering` (revision, offered), `exposed`, `retired`, `left`, `dropped`, `withdrawn` (after the hub's directory watch shows the pool's withdrawal; it names the pool's latest follow and may come after its `dropped`), `listed` (region, condition, properties, and `missing_age_ms`, how long a missing pool has been silent; no `listing` once the pool withdrew) |

Every relay event except `synced` names its class, pool, and generation. `listed` names the pool's latest follow, or generation 0 for a pool the hub never followed. The watch of one follow sends no `listed`. A chooser of an earlier build does not know `withdrawn`: it leaves the pool unread until it reads it again, if it still follows the pool, which by then is draining and not offered. `unfollowed`, `acknowledged`, `properties` on `pool`, `listing` on `follow`, `missing_age_ms` on a listing, and `listed` on `synced` are fields of known events, not kinds of their own, so an earlier reader ignores them. A reader of an earlier build fails closed for the pool at a `properties` or `listed` event, and reads it again. A later build adds to an event only as an optional field.

## Limits and timing

| What | Value | Where |
|---|---|---|
| Event size a reader accepts | 4 MiB | `WatchLimits::EVENT` |
| Snapshot size a reader accepts | 64 MiB | `WatchLimits::SNAPSHOT` |
| Time from asking to `synced` | 30 s | `WatchLimits::SYNCED_WITHIN` |
| Wait of a relay watch for the directory's first snapshots, before its own | 2 s, fixed | `LISTED_WAIT` in `crates/gdl-hub/src/follow/relay.rs` |
| Idle time before a reader ends a watch | 30 s without a byte | `PoolWatcher` |
| Keep-alive | every 10 s | the hub |
| Events a watcher may fall behind | 1024 by default, then its watch ends | `[pool] watch_buffer` |
| Concurrent watches: pool's watch, relay, one follow | 2048, 2048, 16384 by default; beyond that 503 with `Retry-After: 1` | `[pool.watch_caps] pool`, `relay`, `follow` |
| Quorum hold | 2 s by default | `[pool] hold_ms` |
| Record heartbeat, lease | 10 s, 30 s by default | `[pool] heartbeat_ms`, `lease_ms` |
| Load summary period | 10 s by default, at least 1 s | `[pool] summary_ms` |
| Keyed records the table holds | 1,000,000 by default, at least 1 | `[pool] keyed_capacity` |
| Drain answer wait | up to 5 s | the hub |
| Chooser acknowledgement poll, resend | 50 ms, 500 ms | the runtime |
| Wait for a chooser with no relay watch | 30 s by default | `[pool] lost_chooser_ms` |

A watch whose snapshot is complete is never ended by the snapshot limits. Watches taken in process are not capped. `PoolReadiness::refused_watches` counts each route's refusals.

### Backoff

A reader waits before it opens a watch again (`gdl_watch::Backoff`). Each wait is drawn at random between half its step and the whole step. The step doubles after each watch that fails, or ends before it ran 30 s past its snapshot, up to a cap. A watch that ran that long returns the step to its base. A 503 or a refused connection counts as a failure.

| Reader | Base step | Cap |
|---|---|---|
| An instance's watches of its own hub (relay, one follow, its pool's watch), and its chooser declaration | 100 ms | 1 s |
| A hub's follow link of a child | 250 ms (`PoolTiming::refollow`) | 5 s (`PoolTiming::REFOLLOW_CAP`) |
| A hub's watch of the pool directory | 100 ms | 10 s |
