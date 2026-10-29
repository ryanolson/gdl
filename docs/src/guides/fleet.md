# Deploy a fleet of pools

This guide deploys one graph as three pools on one Docker host: a global pool that runs the frontend, and two model pools that run the engine. It uses the `api` example. [Classes and pools](../concepts/pools.md) explains the terms. [Deploy with Docker Compose](compose-deploy.md) covers a bundle without pools.

You need Docker with Compose, and a spec whose node and message crates come from Git or crates.io, as `examples/api/deploy.toml` does. To run crates that are not pushed, see [Run local sources](#run-local-sources). The bundle is one Compose project, so it runs on one host. Its hubs share no volume: each child pool's hub reports its record to its parent's hub over the bundle's network ([The relay backend](../concepts/pools.md#the-relay-backend)).

## 1. Split the graph into classes

Add classes to the spec:

```toml
[segment.front]
nodes = ["Http"]
[segment.engine]
nodes = ["Engine"]

[class.global]
segments = ["front"]
[class.model_x]
segments = ["engine"]
```

To choose pools by load, add `[pools.model_x] policy = "home_first"`. Without it, choosers take turns (`round_robin`).

## 2. Write the fleet file

```toml
[class.global]
ready = { front = 1 }             # default quorum of each pool of the class

[class.model_x]
ready = { engine = 1 }

[pool.global-us]
class = "global"
region = "us-east-1"

[pool.model-x-01]
class = "model_x"
region = "us-east-1"
home = "global-us"                # the parent pool; required for a class that is sent to

[pool.model-x-02]
class = "model_x"
region = "us-east-1"
home = "global-us"
ready = { engine = 2 }            # overrides the class default
```

`requires = { model_x = 1 }` in `[class.global]` sets how many `ready` children the global pool's health needs (1 of each class by default). A pool needs `layout = "<name>"` when the spec has several layouts. Every key is in [Spec and fleet reference](../appendix/spec-reference.md).

## 3. Check and write the bundle

```bash
cargo gdl check --spec gdl.toml --fleet fleet.toml
cargo gdl compose --spec gdl.toml --fleet fleet.toml --out /tmp/fleet
```

`check` prints `cargo gdl: no problems`, or every problem: a missing or wrong `home`, a parent without a child of each class its class sends to, a `ready` key that is not a segment of the class, a count of 0, an unknown key, and more. `compose` runs the same check, and writes nothing if there is a problem.

For every pool, the bundle has one hub, the service named for the pool, and one service for each segment of the pool's class, named `<pool>_<segment>`. Here that is `global-us`, `global-us_front`, `model-x-01`, `model-x-01_engine`, `model-x-02`, and `model-x-02_engine`. The images are `api-hub`, `api-front`, and `api-engine`.

Each service is configured as its pool through the environment, so pools of one layout and class share one image. The hub of `model-x-02` gets:

```text
GDL_POOL__NAME=model-x-02          GDL_POOL__CLASS=model_x
GDL_POOL__REGION=us-east-1         GDL_POOL__HOME=global-us
GDL_POOL__READY__ENGINE=2          GDL_POOL__SUMMARY_MS=10000
GDL_POOL__DIRECTORY=relay          GDL_POOL__PARENT_HUB=global-us:1337
GDL_POOL__HUB=model-x-02:1337
```

`GDL_POOL__PARENT_HUB` is the parent's hub, named for its service. The global pool's hub has none: it reports nowhere. Instead it gets `GDL_POOL__STATE_DIR=/var/lib/gdl-hub`, where it keeps its children's records, and mounts its own volume there, `global-us-state`. No other service mounts a volume. A segment gets the same variables as its hub, plus `GDL_HUB__HOST` naming its pool's hub.

## 4. Run it

```bash
cd /tmp/fleet
docker compose up --build
```

The bundle publishes no hub port. To read a pool's health, ask from a container on the bundle's network, which is named for the bundle's directory:

```bash
docker run --rm --network fleet_default curlimages/curl:8.10.1 -s http://global-us:1337/v1/ext/gdl-pool/health
```

The global pool answers 200 once its own quorum holds and a model pool is `ready` and followed. `docker compose down` stops the fleet. The global pool's hub keeps its children's records in its volume, so after `docker compose up` again it still holds them, a model pool that does not come back included, until that pool withdraws. `docker compose down --volumes` removes them.

## Run local sources

To test crates that are not pushed, add `--local`. The spec must name its node and message crates and gdl by path, as `examples/api/gdl.toml` does. Add the classes of step 1 to it, and run from the repository's root:

```bash
cargo gdl compose --local --spec examples/api/gdl.toml --fleet fleet.toml --out target/fleet
cd target/fleet
docker compose up --build
```

The bundle has the same services, hubs, and variables as above. Each image builds from the repository as it is on this machine: the hub from `[gdl] path`, and each segment from the crates that the spec names by path. One service still builds each image, and the pools of one class run it. Write the bundle under the repository's `target/`, since the build context must hold it. The bundle builds only on this machine ([Local sources](compose-deploy.md#local-sources)).

## How the bundle starts

- **Children report to their parent.** A child pool's hub reports its record to its parent's hub at `<parent>:1337`. A parent's hub takes reports before its own health answers 200, so the start order of the hubs does not matter. A child whose parent's hub is not up yet reports again on its next change of state or heartbeat.
- **One hub per pool.** A pool's record has one writer. Each hub's service names its container (`<graph>-<pool>`), so Compose refuses to scale it, and a host runs one copy of a bundle at a time. Compose's default recreate stops the old hub before it starts the new one.
- **Waits are on liveness.** A segment waits only for its own pool's hub to be healthy, by the hub's own liveness check (`--healthcheck`). Nothing waits for pool health: a parent's pool health answers 503 until its children are ready, so a wait on it would deadlock a fleet that starts together.
- **Ports.** A segment that publishes a fixed, nonzero host port (`HOST:CONTAINER`) may run in one pool only. One whose host port Docker chooses (`CONTAINER`, `0:CONTAINER`, or `IP::CONTAINER`) runs in any number. `[deployment.segment.<name>] config` cannot set `[pool]`.
- **Secrets.** Each pool's service of a segment mounts the segment's secrets, from the one file the spec names ([Secrets](compose-deploy.md#secrets)).

## Add a pool

Add a `[pool.<name>]` table with its `class`, `region`, and `home`. Run `check`, then write the bundle again into a fresh directory, since `compose` refuses one that exists. Start it under the running bundle's project name, so that Compose updates the running fleet and keeps its network and `pools` volume:

```bash
cargo gdl compose --spec gdl.toml --fleet fleet.toml --out /tmp/fleet-2
cd /tmp/fleet-2
docker compose -p fleet up -d --build
```

No other pool's configuration changes. The new pool's hub reports its record to its parent's hub at once, and again when its quorum has held for 2 s. Its parent's hub then follows it, and the parent's choosers start offering to it.

## Drain a pool

Send `POST /v1/ext/gdl-pool/drain` to the pool's hub on its control listener (port 8337):

```bash
docker run --rm --network fleet_default curlimages/curl:8.10.1 -s -X POST http://model-x-02:8337/v1/ext/gdl-pool/drain
```

The hub answers 202 with the pool's health, showing `draining`. The parent stops offering to the pool, the pool's instances finish what they hold and leave, and the hub withdraws the pool's record ([Draining a pool](../concepts/pool-routing.md#draining-a-pool)). Then remove the pool's table from the fleet file, write the bundle again into a fresh directory, and from there run `docker compose -p fleet up -d --remove-orphans`, as in [Add a pool](#add-a-pool). A drain cannot be cancelled. A global pool's hub answers 409.

## Remove a pool whose hub is gone

A pool whose hub is gone cannot drain. Until something says otherwise, its parent holds it missing, not removed. Withdraw it on its parent's hub, here `global-us` for `model-x-02`:

```bash
docker run --rm --network fleet_default curlimages/curl:8.10.1 -s -X POST \
  http://global-us:8337/v1/ext/gdl-pool/children/model_x/model-x-02/withdraw
```

The parent answers 204 once it has written the withdrawal, and its choosers hold the pool withdrawn. If the pool was scaled to 0 on purpose and will come back, say that instead, with `.../children/model_x/model-x-02/scale-to-zero`: the pool then reads as scaled to 0, not missing. A late record from the pool's old hub cannot undo either. A new hub of the pool writes past both ([A pool whose hub is gone](../concepts/pools.md#a-pool-whose-hub-is-gone)).

## Revoke a model

Revoke one model of a pool on the pool's own hub. The pool's other models serve on:

```bash
docker run --rm --network fleet_default curlimages/curl:8.10.1 -s -X POST \
  -H 'content-type: application/json' -d '{"model": "acme-legal"}' \
  http://model-x-02:8337/v1/ext/gdl-pool/models/revoke
```

The hub answers 204 once the pool's record leaves the model out. It stays out, whatever the pool's instances say and across a restart of the hub, until you lift it with `.../models/lift` and the same body ([Revoking a model](../concepts/pools.md#revoking-a-model)).
