# Cell hub routes and watch events

A cell's hub serves the `gdl-cell` extension's routes under `/v1/ext/gdl-cell`. The public listener is port 1337 and the control listener is port 8337. [Classes and cells](../concepts/cells.md) and [Following and choosing cells](../concepts/cell-routing.md) describe what each route does. Field-level detail is the rustdoc of `gdl_watch` (wire types, routes, client) and `gdl_hub` (`CellReadiness`, `CellTiming`, `CellHealth`). A hub process reads its timing from the `[cell]` keys in the table below, as `GDL_CELL__*` variables ([Segment settings](../operations/configuration.md)).

## Routes

| Method and path | Listener | Credential | Body | Answers |
|---|---|---|---|---|
| `GET /health` | public | none | none | 200 when the cell is healthy, 503 otherwise, both with a `CellHealth` body |
| `GET /watch` | public | none | optional query `?class=&cell=&run=&generation=` (`gdl_watch::Follower`) | the cell's watch as server-sent events (`CellWatchEvent`) |
| `GET /follows` | public | none | optional query `?instance=` (`gdl_watch::RelayWatcher`), the chooser that watches | the relay as server-sent events (`FollowEvent`) |
| `GET /follows/{class}/{cell}` | public | none | none | one cell's follows: its closing generations, then its current one, then `synced`, then each relay event that names the cell |
| `POST /release` | public | none | `Follower` | 204; the watch no longer counts as a follower |
| `POST /avoided` | public | none | `Avoided` | 204; counted only from a watch that counted when the instance retired |
| `POST /report` | public | none | `Report`: a child's record or withdrawal | 200 with a `ReportReply` (`interval_ms`, when the parent expects the next report); 400 with a `ReportRefusal` for a malformed one; 409 with a `ReportRefusal` for one the parent refuses. Served only by a hub under the `relay` backend, whether or not its cell is healthy |
| `GET /report/{class}/{cell}` | public | none | none | 200 with the `CellRecord` the parent holds for that child, expired or not; 404 when it holds none. Served only by a hub under the `relay` backend. A restarted child's hub reads it before its first report |
| `POST /drain` | control | none | none | 202 with the health body; 409 for a cell whose class no hop enters |
| `POST /presence` | control | none | `SetPresence`: `{"presence": "serving"}` or `{"presence": "scaled_to_zero"}` | 204 once the directory has stored a record written after the request with that presence; 503 when it has not within 5 s, or a later request replaced it |
| `POST /models/revoke` | control | none | a model's key: `{"model": "acme-legal"}`, with `routing_key` when it has one | 204 once the directory has stored a record that leaves the model out; 400 for a malformed model; 503 when it has not within 5 s, or a later request lifted it |
| `POST /models/lift` | control | none | a model's key | 204 once the directory has stored a record that no longer revokes the model; 400 and 503 as for `revoke` |
| `POST /children/{class}/{cell}/withdraw` | control | none | none | 204 once the directory has stored the child's withdrawal, at an epoch past its last record; 404 when the hub holds no record of a child of that name and class; 409 when the directory refuses it; 503 when the directory cannot be read or written |
| `POST /children/{class}/{cell}/scale-to-zero` | control | none | none | as `withdraw`, for the child's last record written again with `scaled_to_zero` |
| `POST /instances/{id}/load` | control | the instance's | `LoadReport`, with the instance's `models` when its nodes set them | 204; 409 unless the instance is a usable entry instance, and before the hub has read its cell's last record |
| `POST /instances/{id}/chooser` | control | the instance's | none | 204; 409 unless the instance is of a segment that chooses cells |
| `POST /instances/{id}/unoffered` | control | the instance's | `Unoffered` | 204, whether or not it answers the current revision; 409 from an instance that is not a chooser |
| `POST /instances/{id}/avoids` | control | the instance's | `Avoids` | 204; 409 from an instance that is not a chooser |

- Refusals on the credentialed routes are 409, not 401 or 404, because velo-hub's client drops its registration on those.
- `release` and `avoided` are not authenticated. Nothing checks the names in a `Follower`, or the chooser a relay watch names.
- The hub waits for a chooser's acknowledgements while it has a relay watch open in its name, and for `[cell] lost_chooser_ms` after its last one closed or after it declared.
- `report` carries no credential either. The parent writes only what it verifies: a record of the right shape whose `home` is the parent's cell, whose contract and placement hashes are the parent's, and that is newer than what it holds. A withdrawal is written only over a record the parent holds. Any other withdrawal is answered 200 and writes nothing.
- A watch that names no follower counts for nothing in the drain and retirement waits. A watch that closes without a release still counts. Once the cell's lease has passed since it closed, it no longer shows as a follower, but it never counts as a release.
- A watch that arrives before the hub first stores the cell's record waits up to 5 s, then gets 503. Every watch ends when the hub stops, or once a drained cell's record is withdrawn. A watch after that gets 503.
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
| Cell's watch (`CellWatchEvent`) | `cell` (name, class, epoch, state, load, and `models`, the cell's model entries and their version); `state` with `unfollowed: true` if already sent; `retired`; `ready` (segment, `PeerInfo`); `synced` | `state`; `load` once a period while there is a summary, and once when there is none; `models` when the list changes; `ready`; `retired`; `retired` again with `acknowledged: true`; `left`; `state` again with `unfollowed: true` |
| Relay (`FollowEvent`) | for each follow: `follow` (class, cell, generation, region, state, load, `load_age_ms`, how long ago the hub received the load, and the current follow's `listing`), `offering` if not offered, `retired`, `exposed`; then `listed` for each listed cell with no follow; then one `synced`, with `listed: true` once the hub has read the directory's first snapshot of every class it sends to | `state`, `load`, `offering` (revision, offered), `exposed`, `retired`, `left`, `dropped`, `withdrawn` (after the hub's directory watch shows the cell's withdrawal; it names the cell's latest follow and may come after its `dropped`), `listed` (region, condition, models; no `listing` once the cell withdrew) |

Every relay event except `synced` names its class, cell, and generation. `listed` names the cell's latest follow, or generation 0 for a cell the hub never followed. The watch of one follow sends no `listed`. A chooser of an earlier build does not know `withdrawn`: it leaves the cell unread until it reads it again, if it still follows the cell, which by then is draining and not offered. `unfollowed`, `acknowledged`, `models` on `cell`, `listing` on `follow`, and `listed` on `synced` are fields of known events, not kinds of their own, so an earlier reader ignores them. A reader of an earlier build fails closed for the cell at a `models` or `listed` event, and reads it again. A later build adds to an event only as an optional field.

## Limits and timing

| What | Value | Where |
|---|---|---|
| Event size a reader accepts | 4 MiB | `WatchLimits::EVENT` |
| Snapshot size a reader accepts | 64 MiB | `WatchLimits::SNAPSHOT` |
| Time from asking to `synced` | 30 s | `WatchLimits::SYNCED_WITHIN` |
| Wait of a relay watch for the directory's first snapshots, before its own | 2 s, fixed | `LISTED_WAIT` in `crates/gdl-hub/src/follow/relay.rs` |
| Idle time before a reader ends a watch | 30 s without a byte | `CellWatcher` |
| Keep-alive | every 10 s | the hub |
| Events a watcher may fall behind | 1024 by default, then its watch ends | `[cell] watch_buffer` |
| Concurrent watches: cell's watch, relay, one follow | 2048, 2048, 16384 by default; beyond that 503 with `Retry-After: 1` | `[cell.watch_caps] cell`, `relay`, `follow` |
| Quorum hold | 2 s by default | `[cell] hold_ms` |
| Record heartbeat, lease | 10 s, 30 s by default | `[cell] heartbeat_ms`, `lease_ms` |
| Load summary period | 10 s by default, at least 1 s | `[cell] summary_ms` |
| Keyed records the table holds | 1,000,000 by default, at least 1 | `[cell] keyed_capacity` |
| Drain answer wait | up to 5 s | the hub |
| Chooser acknowledgement poll, resend | 50 ms, 500 ms | the runtime |
| Wait for a chooser with no relay watch | 30 s by default | `[cell] lost_chooser_ms` |

A watch whose snapshot is complete is never ended by the snapshot limits. Watches taken in process are not capped. `CellReadiness::refused_watches` counts each route's refusals.

### Backoff

A reader waits before it opens a watch again (`gdl_watch::Backoff`). Each wait is drawn at random between half its step and the whole step. The step doubles after each watch that fails, or ends before it ran 30 s past its snapshot, up to a cap. A watch that ran that long returns the step to its base. A 503 or a refused connection counts as a failure.

| Reader | Base step | Cap |
|---|---|---|
| An instance's watches of its own hub (relay, one follow, its cell's watch), and its chooser declaration | 100 ms | 1 s |
| A hub's follow link of a child | 250 ms (`CellTiming::refollow`) | 5 s (`CellTiming::REFOLLOW_CAP`) |
| A hub's watch of the cell directory | 100 ms | 10 s |
