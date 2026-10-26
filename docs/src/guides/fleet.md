# Deploy a fleet of cells

This guide deploys one graph as three cells on one Docker host: a global cell that runs the frontend, and two model cells that run the engine. It uses the `api` example. [Classes and cells](../concepts/cells.md) explains the terms. [Deploy with Docker Compose](compose-deploy.md) covers a bundle without cells.

You need Docker with Compose, and a spec whose node and message crates come from Git or crates.io, as `examples/api/deploy.toml` does. To run crates that are not pushed, see [Run local sources](#run-local-sources). The bundle is one Compose project, so it runs on one host. Its hubs share no volume: each child cell's hub reports its record to its parent's hub over the bundle's network ([The relay backend](../concepts/cells.md#the-relay-backend)).

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

To choose cells by load, add `[cells.model_x] policy = "home_first"`. Without it, choosers take turns (`round_robin`).

## 2. Write the fleet file

```toml
[class.global]
ready = { front = 1 }             # default quorum of each cell of the class

[class.model_x]
ready = { engine = 1 }

[cell.global-us]
class = "global"
region = "us-east-1"

[cell.model-x-01]
class = "model_x"
region = "us-east-1"
home = "global-us"                # the parent cell; required for a class that is sent to

[cell.model-x-02]
class = "model_x"
region = "us-east-1"
home = "global-us"
ready = { engine = 2 }            # overrides the class default
```

`requires = { model_x = 1 }` in `[class.global]` sets how many `ready` children the global cell's health needs (1 of each class by default). A cell needs `layout = "<name>"` when the spec has several layouts. Every key is in [Spec and fleet reference](../appendix/spec-reference.md).

## 3. Check and write the bundle

```bash
cargo gdl check --spec gdl.toml --fleet fleet.toml
cargo gdl compose --spec gdl.toml --fleet fleet.toml --out /tmp/fleet
```

`check` prints `cargo gdl: no problems`, or every problem: a missing or wrong `home`, a parent without a child of each class its class sends to, a `ready` key that is not a segment of the class, a count of 0, an unknown key, and more. `compose` runs the same check, and writes nothing if there is a problem.

For every cell, the bundle has one hub, the service named for the cell, and one service for each segment of the cell's class, named `<cell>_<segment>`. Here that is `global-us`, `global-us_front`, `model-x-01`, `model-x-01_engine`, `model-x-02`, and `model-x-02_engine`. The images are `api-hub`, `api-front`, and `api-engine`.

Each service is configured as its cell through the environment, so cells of one layout and class share one image. The hub of `model-x-02` gets:

```text
GDL_CELL__NAME=model-x-02          GDL_CELL__CLASS=model_x
GDL_CELL__REGION=us-east-1         GDL_CELL__HOME=global-us
GDL_CELL__READY__ENGINE=2          GDL_CELL__SUMMARY_MS=10000
GDL_CELL__DIRECTORY=relay          GDL_CELL__PARENT_HUB=global-us:1337
GDL_CELL__HUB=model-x-02:1337
```

`GDL_CELL__PARENT_HUB` is the parent's hub, named for its service. The global cell's hub has none: it reports nowhere. A segment gets the same variables, plus `GDL_HUB__HOST` naming its cell's hub.

## 4. Run it

```bash
cd /tmp/fleet
docker compose up --build
```

The bundle publishes no hub port. To read a cell's health, ask from a container on the bundle's network, which is named for the bundle's directory:

```bash
docker run --rm --network fleet_default curlimages/curl:8.10.1 -s http://global-us:1337/v1/ext/gdl-cell/health
```

The global cell answers 200 once its own quorum holds and a model cell is `ready` and followed. `docker compose down` stops the fleet. A parent's hub keeps its children's records in memory, so each deployment starts with none.

## Run local sources

To test crates that are not pushed, add `--local`. The spec must name its node and message crates and gdl by path, as `examples/api/gdl.toml` does. Add the classes of step 1 to it, and run from the repository's root:

```bash
cargo gdl compose --local --spec examples/api/gdl.toml --fleet fleet.toml --out target/fleet
cd target/fleet
docker compose up --build
```

The bundle has the same services, hubs, and variables as above. Each image builds from the repository as it is on this machine: the hub from `[gdl] path`, and each segment from the crates that the spec names by path. One service still builds each image, and the cells of one class run it. Write the bundle under the repository's `target/`, since the build context must hold it. The bundle builds only on this machine ([Local sources](compose-deploy.md#local-sources)).

## How the bundle starts

- **Children report to their parent.** A child cell's hub reports its record to its parent's hub at `<parent>:1337`. A parent's hub takes reports before its own health answers 200, so the start order of the hubs does not matter. A child whose parent's hub is not up yet reports again on its next change of state or heartbeat.
- **One hub per cell.** A cell's record has one writer. Each hub's service names its container (`<graph>-<cell>`), so Compose refuses to scale it, and a host runs one copy of a bundle at a time. Compose's default recreate stops the old hub before it starts the new one.
- **Waits are on liveness.** A segment waits only for its own cell's hub to be healthy, by the hub's own liveness check (`--healthcheck`). Nothing waits for cell health: a parent's cell health answers 503 until its children are ready, so a wait on it would deadlock a fleet that starts together.
- **Ports.** A segment that publishes a fixed, nonzero host port (`HOST:CONTAINER`) may run in one cell only. One whose host port Docker chooses (`CONTAINER`, `0:CONTAINER`, or `IP::CONTAINER`) runs in any number. `[deployment.segment.<name>] config` cannot set `[cell]`.
- **Secrets.** Each cell's service of a segment mounts the segment's secrets, from the one file the spec names ([Secrets](compose-deploy.md#secrets)).

## Add a cell

Add a `[cell.<name>]` table with its `class`, `region`, and `home`. Run `check`, then write the bundle again into a fresh directory, since `compose` refuses one that exists. Start it under the running bundle's project name, so that Compose updates the running fleet and keeps its network and `cells` volume:

```bash
cargo gdl compose --spec gdl.toml --fleet fleet.toml --out /tmp/fleet-2
cd /tmp/fleet-2
docker compose -p fleet up -d --build
```

No other cell's configuration changes. The new cell's hub reports its record to its parent's hub at once, and again when its quorum has held for 2 s. Its parent's hub then follows it, and the parent's choosers start offering to it.

## Drain a cell

Send `POST /v1/ext/gdl-cell/drain` to the cell's hub on its control listener (port 8337):

```bash
docker run --rm --network fleet_default curlimages/curl:8.10.1 -s -X POST http://model-x-02:8337/v1/ext/gdl-cell/drain
```

The hub answers 202 with the cell's health, showing `draining`. The parent stops offering to the cell, the cell's instances finish what they hold and leave, and the hub withdraws the cell's record ([Draining a cell](../concepts/cell-routing.md#draining-a-cell)). Then remove the cell's table from the fleet file, write the bundle again into a fresh directory, and from there run `docker compose -p fleet up -d --remove-orphans`, as in [Add a cell](#add-a-cell). A drain cannot be cancelled. A global cell's hub answers 409.
