# Cell hub routes and watch events

A cell's hub serves the `gdl-cell` extension's routes under `/v1/ext/gdl-cell`. The public listener is port 1337 and the control listener is port 8337. [Classes and cells](../concepts/cells.md) and [Following and choosing cells](../concepts/cell-routing.md) describe what each route does. Field-level detail is the rustdoc of `gdl_watch` (wire types, routes, client) and `gdl_hub` (`CellReadiness`, `CellTiming`, `CellHealth`).

## Routes

| Method and path | Listener | Credential | Body | Answers |
|---|---|---|---|---|
| `GET /health` | public | none | none | 200 when the cell is healthy, 503 otherwise, both with a `CellHealth` body |
| `GET /watch` | public | none | optional query `?class=&cell=&run=&generation=` (`gdl_watch::Follower`) | the cell's watch as server-sent events (`CellWatchEvent`) |
| `GET /follows` | public | none | none | the relay as server-sent events (`FollowEvent`) |
| `GET /follows/{class}/{cell}` | public | none | none | one cell's follows: its closing generations, then its current one, then `synced`, then each relay event that names the cell |
| `POST /release` | public | none | `Follower` | 204; the watch no longer counts as a follower |
| `POST /avoided` | public | none | `Avoided` | 204; counted only from a watch that counted when the instance retired |
| `POST /report` | public | none | `Report`: a child's record or withdrawal | 200 with a `ReportReply` (`interval_ms`, when the parent expects the next report); 400 with a `ReportRefusal` for a malformed one; 409 with a `ReportRefusal` for one the parent refuses. Served only by a hub under the `relay` backend, whether or not its cell is healthy |
| `POST /drain` | control | none | none | 202 with the health body; 409 for a cell whose class no hop enters |
| `POST /instances/{id}/load` | control | the instance's | `LoadReport` | 204; 409 unless the instance is a usable entry instance |
| `POST /instances/{id}/chooser` | control | the instance's | none | 204; 409 unless the instance is of a segment that chooses cells |
| `POST /instances/{id}/unoffered` | control | the instance's | `Unoffered` | 204, whether or not it answers the current revision; 409 from an instance that is not a chooser |
| `POST /instances/{id}/avoids` | control | the instance's | `Avoids` | 204; 409 from an instance that is not a chooser |

- Refusals on the credentialed routes are 409, not 401 or 404, because velo-hub's client drops its registration on those.
- `release` and `avoided` are not authenticated. Nothing checks the names in a `Follower`.
- `report` carries no credential either. The parent writes only what it verifies: a record of the right shape whose `home` is the parent's cell, whose contract and placement hashes are the parent's, and that is newer than what it holds. A withdrawal is written only over a record the parent holds. Any other withdrawal is answered 200 and writes nothing.
- A watch that names no follower counts for nothing in the drain and retirement waits. A watch that closes without a release still counts. Once the cell's lease has passed since it closed, it no longer shows as a follower, but it never counts as a release.
- A watch that arrives before the hub first stores the cell's record waits up to 5 s, then gets 503. Every watch ends when the hub stops, or once a drained cell's record is withdrawn. A watch after that gets 503.
- `GraphAdmission` serves `GET /v1/ext/gdl/members`, whose `PeerInfo` values the watch events carry.

## Watch events

Each server-sent event's data is one JSON value tagged by `event`.

| Watch | Snapshot, in order | Changes |
|---|---|---|
| Cell's watch (`CellWatchEvent`) | `cell` (name, class, epoch, state, load); `state` with `unfollowed: true` if already sent; `retired`; `ready` (segment, `PeerInfo`); `synced` | `state`; `load` once a period while there is a summary, and once when there is none; `ready`; `retired`; `retired` again with `acknowledged: true`; `left`; `state` again with `unfollowed: true` |
| Relay (`FollowEvent`) | for each follow: `follow` (class, cell, generation, region, state, load, and `load_age_ms`, how long ago the hub received the load), `offering` if not offered, `retired`, `exposed`; then one `synced` | `state`, `load`, `offering` (revision, offered), `exposed`, `retired`, `left`, `dropped`, `withdrawn` (after the hub's directory watch shows the cell's withdrawal; it names the cell's latest follow and may come after its `dropped`) |

Every relay event except `synced` names its class, cell, and generation. A chooser of an earlier build does not know `withdrawn`: it leaves the cell unread until it reads it again, if it still follows the cell, which by then is draining and not offered. `unfollowed` and `acknowledged` are fields of known events, not kinds of their own, so an earlier reader ignores them. A later build adds to an event only as an optional field.

## Limits and timing

| What | Value | Where |
|---|---|---|
| Event size a reader accepts | 4 MiB | `WatchLimits::EVENT` |
| Snapshot size a reader accepts | 64 MiB | `WatchLimits::SNAPSHOT` |
| Time from asking to `synced` | 30 s | `WatchLimits::SYNCED_WITHIN` |
| Idle time before a reader ends a watch | 30 s without a byte | `CellWatcher` |
| Keep-alive | every 10 s | the hub |
| Events a watcher may fall behind | 1024, then its watch ends | `CellTiming::watch_buffer` |
| Concurrent watches: cell's watch, relay, one follow | 2048, 2048, 16384; beyond that 503 with `Retry-After: 1` | `WatchCaps` |
| Quorum hold | 2 s | `Quorum::DEFAULT_HOLD` |
| Record heartbeat, lease | 10 s, 30 s | `CellTiming`, `CellRecord::DEFAULT_LEASE` |
| Load summary period | 10 s by default, at least 1 s | `[cell] summary_ms` |
| Drain answer wait | up to 5 s | the hub |
| Chooser acknowledgement poll, resend | 50 ms, 500 ms | the runtime |

A watch whose snapshot is complete is never ended by the snapshot limits. Watches taken in process are not capped. `CellReadiness::refused_watches` counts each route's refusals.

### Backoff

A reader waits before it opens a watch again (`gdl_watch::Backoff`). Each wait is drawn at random between half its step and the whole step. The step doubles after each watch that fails, or ends before it ran 30 s past its snapshot, up to a cap. A watch that ran that long returns the step to its base. A 503 or a refused connection counts as a failure.

| Reader | Base step | Cap |
|---|---|---|
| An instance's watches of its own hub (relay, one follow, its cell's watch), and its chooser declaration | 100 ms | 1 s |
| A hub's follow link of a child | 250 ms (`CellTiming::refollow`) | 5 s (`CellTiming::REFOLLOW_CAP`) |
| A hub's watch of the cell directory | 100 ms | 10 s |
