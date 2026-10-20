# Compose deployment

The hub runs Velo's registration service with a GDL admission extension. Its input is the typed manifest and placement. Each generated segment presents its name and the contract and placement hashes compiled into that binary. Missing or mismatched registrations are rejected before they enter membership.

Compose starts the hub first and waits for its HTTP health check. It then starts all segments together. Each segment waits for one admitted peer per segment, installs its handlers, and publishes a readiness service. The frontend serves after every expected peer has announced readiness. Ordering the segments themselves would deadlock this barrier.

This implementation takes a roster snapshot at startup. It does not support replica churn or hub recovery. A lease failure ends the segment process. Replacing a segment requires restarting the stack. Request dispatch still uses the runtime's configured router policy; responses retain their stream anchors.

The build bundle snapshots non-ignored Git worktree files for local dependencies. Symlinks are omitted. The copied source retains its original absolute path inside the builder, so generated path dependencies resolve without requiring that directory on the Docker host. Runtime images contain only their executable and configuration. A segment can override the deployment's runtime base image; the hub has its own override. The Rust build image is independent.

## Validation on 2026-10-19

- Velo: the Git revision `Cargo.toml` pins (0.21.0 at `1bbeb88` on 2026-10-20).
- Hub integration: rejected a mismatched contract, discovered two admitted peers, and completed the readiness barrier over OS-assigned ports.
- Generator unit tests: 86 passed. Model unit tests: 2 passed.
- Deployment generation tests: 2 passed. Registration identity test: 1 passed.
- Web API tests: 25 passed. Editor tests: 67 passed.
- Runtime service tests: 8 passed, including four segments and independent response paths.
- Workspace clippy passed with all targets and all features. Formatting passed.
- Browser: Graph retained 90% zoom after Segments was set to 80%. Deploy displayed four segments and downloaded a bundle with five services, per-segment Dockerfiles, the graph definition, and local sources.
- Built all four generated binaries from the extracted source snapshot. Ran them with the hub as separate native processes. All four announced ready. The chat, responses, and messages HTTP endpoints each returned the two expected NDJSON stream items through the shared Echo sink.


Docker is not installed on this workstation. The owner will run `docker compose up --build` on another machine. No container build or execution is claimed by the native checks.

## 2026-10-19 addendum: portable package sources

This replaces the local source snapshot design above. Deploy accepts only Git or crates.io node and message packages. GDL and the hub must also use a Git or registry dependency. The generated Dockerfiles fetch dependencies through Cargo, and the bundle contains only generated files. Local packages remain available for development and graph editing, but deployment returns an error that names the packages to re-import.

The public repository is `https://github.com/ryanolson/gdl`. `examples/api/deploy.toml` pins the API example and GDL to the same commit. Preview and download still share one generated file snapshot.

## 2026-10-20 addendum: readiness protocol, dynamic instance counts

This supersedes the one-replica rule above and records the owner's rulings of 2026-10-20. The roster is still a snapshot taken at startup; what changed is what it holds and when it is taken.

- **Live, loaded, serving.** A segment registers with the hub as soon as its nodes are built, so the hub knows the process is live while the nodes load. Every node has `Build::ready`, immediate by default; once all resolve, the segment announces `loaded`. After `start` installs its handlers it announces `serving`.
- **Rosters.** A segment waits for at least `[min_replicas]` loaded instances of each segment (one by default) and takes those as its roster. Every instance announces before it waits, so none waits for another that is waiting for it: a forward path that leaves a segment and comes back makes two segments send to each other. It then waits for every instance in its roster of the segments it sends to (`Layout::sends_to`) to be serving. Rosters need not agree, since the roster is held from the launch until that wait is over: a request that reaches an instance earlier waits at its roster, bounded by the runtime's setup timeout, instead of leaving for a peer whose handlers are not installed. How many instances a segment has is the deployment's business: the hub never refuses more, and `[instances]`, the exact count a roster directory needs, is an error under a hub.
- **A roster directory** waits the same way: `[instances]` fixes the roster, each instance publishes that it serves, then waits for the instances of the segments it sends to.
- **Where the launch lives.** `gdl::ProcessConfig` and `gdl::Launch` in the runtime own settings, discovery, the barriers, signals, and the hub lease; a generated binary declares what it hosts and calls them. The hub's ports are `gdl_model::HUB_PUBLIC_PORT` and `HUB_CONTROL_PORT`, named once.
- **Settings.** Only the settings' own keys are read from `GDL_`, so the build scripts' `GDL_PROFILE` and `GDL_IMAGE_PREFIX` cannot stop a segment. Node tables are addressed by their names whatever the case of the variable. Exactly one of `roster` and `hub` is set. SIGTERM stops a segment as SIGINT does.
- **Next (owner, 2026-10-20).** A health endpoint on each segment that answers live from the start and ready only once the graph is loaded and the barriers have passed. Built in milestone 20 (ruling 104): the system listener answers `/live` and `/health` on dynamo's port and paths, and the Compose bundle sets `[system] port = 9090` and gives each segment a health check through `segment --healthcheck`. No segment waits on another's health.

