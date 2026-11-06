# Deploy a fleet on Slurm

A Slurm site runs each pool of a fleet as one job, which [srt-slurm](https://github.com/NVIDIA/srt-slurm) submits and runs. `cargo gdl slurm` writes the srt-slurm recipes of a fleet's Slurm sites, and the settings each job reads. It needs no Docker, no srt-slurm, and no cluster. No command submits the jobs yet: apply the recipes with `srtctl` by hand ([Limitations](../operations/limitations.md#no-command-runs-a-slurm-site-yet)).

The recipes are written for srt-slurm at commit `acfb4dcc`.

## The site and its pools

A Slurm site is a `[cluster.<name>]` table with `kind = "slurm"` ([Deploy a fleet of pools](fleet.md), [reference](../appendix/spec-reference.md#fleettoml)). Its account, default partition, and network interface stay in srt-slurm's `srtslurm.yaml` on the cluster, never in the fleet file.

```toml
[fleet]
name = "api"

[cluster.lab]
kind = "slurm"
region = "slurm-lab"
srtctl = "/shared/srt-slurm"
run_dir = "/shared/gdl-runs"      # a file system the login node and every compute node share
registry = "registry.example/gdl"
platform = "linux/arm64"
time_limit = "08:00:00"

[directory]                       # required: the pools find each other through DynamoDB Local
backend = "dynamodb"
records_table = "gdl-pools"
lists_table = "gdl-pool-lists"
local = true
cluster = "lab"                   # the site that runs the directory's job

[pool.model-x-01]
class = "model_x"
cluster = "lab"
parents = "global"
slurm = { partition = "gpu", spare_nodes = 1 }
```

The fleet must have a `[directory]` with `local = true`. A child pool's hub under the relay needs its parent's hub's address, which a job learns only once it runs. A Slurm job has no AWS role to reach DynamoDB on AWS with.

## Build the images

The images take gdl as the spec's `[gdl]` names it. The recipes set what the generator's own gdl reads, so pin a gdl at least as recent as the generator, or take it by `path` ([Limitations](../operations/limitations.md#slurm-recipes-need-a-gdl-that-reads-what-they-set)).

```bash
cargo gdl images --spec gdl.toml --fleet fleet.toml --push --out images.json
```

A Slurm site's images are built on the site's `base_image`, `debian:bookworm-slim` unless it says otherwise, and its hub's on its `hub_image`. srt-slurm runs every command under `bash -c`, and the spec's default base image has no shell. A segment's own `[deployment.segment.<name>] base_image` still wins. A Kubernetes cluster that shares the site's registry and platform gets images of its own, on the spec's base. The spec's `[deployment.codegen.<arch>]` table reaches a site's segment images as it reaches a cluster's.

## Render the recipes

```bash
cargo gdl slurm --spec gdl.toml --fleet fleet.toml --images images.json --out slurm
```

`--images` is required: a site pulls each image by its digest. `--tag` looks each image up under another tag, as `cargo gdl k8s --tag` does.

The output has one directory per site, which holds what goes in `<run_dir>/<fleet>/` on the cluster:

```text
slurm/lab/
  directory.yaml               # the DynamoDB Local's job
  model-x-01/
    pool.yaml                  # the pool's job
    join-engine.yaml           # a job that adds an engine instance to the running pool
    deployment.json            # the hub's settings
    engine.toml                # each segment's settings
```

Each recipe is one JSON object, which YAML reads as it is. Each job is named `gdl-<fleet>-<pool>`, `gdl-<fleet>-<pool>-join-<segment>`, or `gdl-<fleet>-directory`, and its sbatch comment says what it is: `gdl:<fleet>:<pool>:pool:<hash>`, `gdl:<fleet>:<pool>:join:<hash>`, or `gdl:<fleet>:-:directory:<hash>`. The hash is the first 16 hex digits of a sha256 of the recipe and the settings it mounts, so the same render gives the same hash. A job's name alone does not tell its kind: a pool may be named `x-join-engine`.

## A pool's job

A pool's recipe has no frontend (`frontend.type: none`) and these services, which srt-slurm starts in order:

- `hub`, on the head node: `critical` and `terminal`, so the job ends when the hub exits. It is ready when its own `/v1/health` answers 200 on port 1337. It publishes `{node_ip}:1337` as its pool's hub, and drains its pool on the first `SIGTERM` (`GDL_POOL__DRAIN_ON_STOP=true`, for the pool's `stop_wait_ms`).
- one service per segment of the pool's class, in name order, on nodes of its own: one instance per node, as many as the pool's `ready` count. Each finds its hub at `{head_ip}` and gives velo `{node_ip}:0`, a port the OS chooses. A segment with `ready` 0 has no service.
- `spare`, when the pool has `slurm.spare_nodes`: a placeholder that holds those nodes for segments that join inside the job.

Each segment's preamble sets `GLIBC_TUNABLES=glibc.malloc.hugetlb=0` when the job's environment has none, and keeps the user's value when it has one ([Binary](../appendix/spec-reference.md#binary)).

Each segment instance is ready when its system listener answers `/live`, not `/health`. srt-slurm starts one instance at a time and waits for each probe before the next. An instance answers `/health` only once every segment of its class has instances, so a class of two segments would wait forever. `crates/gdl-gen/tests/slurm.rs` starts such a class in srt-slurm's order, and shows the wait on `/health`. The pool's health is its hub's `/v1/ext/gdl-pool/health`.

The job mounts `<run_dir>/<fleet>/<pool>` at `/gdl`. srt-slurm requires a model, so `model.path` is `<run_dir>/<fleet>`, which its preflight requires to be a directory, and nothing reads it.

Every job takes its nodes whole (`exclusive`) and keeps the default ports: 1337 and 8337 for the hub, 9090 for each segment's system listener. A pool's record carries only its hub's public port, so a joiner cannot learn a moved control port, and two jobs on one node would bind the same ports. Inside a job, the hub shares the head node with the first instance of the first segment. The render refuses a node where two processes would bind one port.

A pool's job also carries `no-requeue`, `signal: TERM@<signal_lead_s>`, and `no-kill` unless the site turns it off. A requeued job keeps its id, so an old endpoint file of the directory's would look fresh, and a requeued hub would restart with no registrations.

## Leaving by the job's end time

Each process of a pool's job, and of a joining job, knows when the job ends, and drains before then ([A known leave time](../concepts/lifecycle.md#a-known-leave-time)):

- The job's `host_setup` runs on each node's host before any service starts. It writes the job's end time, in Unix seconds, to `job.<job id>.end` in the pool's directory: `SLURM_JOB_END_TIME` where the cluster's Slurm sets it, else the end `squeue -h -j <job id> -o %e` gives. With neither, as for a job with no time limit, it writes an empty file. Its `teardown` removes the file.
- Each segment instance's preamble reads the file as `[runtime] leave_by` (`GDL_RUNTIME__LEAVE_BY`, in milliseconds). It starts to drain `drain_ms` and `leave_margin_ms` (30 s) before then.
- The hub's preamble reads it as `[pool] leave_by` (`GDL_POOL__LEAVE_BY`), which it reads since it drains on stop. It starts the pool's drain `stop_wait_ms` and `leave_margin_ms` before then: with the defaults, 690 s before the end, 60 s ahead of its instances (`drain_ms` 600 s).

With the default `signal_lead_s`, 900 s, Slurm's `TERM` comes first, and starts the same drains. The leave time drains the job when no signal comes. A recipe's `host_setup` replaces the cluster's `default_host_setup` in `srtslurm.yaml` ([Limitations](../operations/limitations.md#a-slurm-recipe-replaces-the-clusters-default-host-setup)).

## The directory's job

`directory.yaml` runs DynamoDB Local, pinned by digest, on a node of its own, and then the `tables` step beside it. The step removes this job's old endpoint file, runs `gdl-hub --tables`, and only then writes `http://{node_ip}:8000` to `<run_dir>/<fleet>/directory.<job id>.endpoint`, for the tool that submitted the job, and then points `<run_dir>/<fleet>/directory.endpoint`, the fleet's pointer, at that file: a link, written beside its place and renamed into it, so the latest directory job's pointer wins. When the job ends, its teardown removes its own file alone. A pointer to it then dangles, which a process reads as no address, and a later job's pointer is never touched.

No recipe holds the Local's address. Each pool's and joining job's processes mount `<run_dir>/<fleet>` read-only at `/fleet`, and their preambles read the pointer as `GDL_POOL__ENDPOINT` when they start. A process with no pointer, or one that is not `http://<host>:<port>`, does not start, and says so in one line that names the file. So a new directory job moves no pool's recipe hash. A hub keeps the address it started with: after a new directory job, stop the pools and submit them again.

The Local runs on the Slurm site of every Slurm pool, since a job reads the pointer from its own site's `run_dir`: `cargo gdl slurm` refuses a `[directory] cluster` that is another cluster.

The directory's job takes no `signal`. Each pool withdraws its record from the directory as it drains, on the signal before its own time limit. Run the directory's job on a partition with a longer time limit than its pools'.

## A segment that joins a running pool

`join-<segment>.yaml` adds one instance of a segment to a running pool, as a job of its own. Its `resolve` service runs `gdl-hub --resolve <class>/<pool> --write /logs/gdl-hub.toml` when the job starts, not when it is queued, since a queued job may outlive the hub it was queued for. Then the segment starts with that file over its settings. It has no `GDL_HUB__HOST`, which would override the file. The segment is `terminal`, so the job ends when it leaves.

`resolve` waits once it has written the file. srt-slurm probes the file, then asks whether the process still runs: a `resolve` that exited between the two would fail the job. `resolve` is not `critical`: the job's `TERM` before its time limit ends its wait, and srt-slurm would take a critical service's end on a signal as a failure, and cut the segment's drain short. A `resolve` that fails before it writes the file still fails the job, through its probe.

## Apply the recipes by hand

1. Copy `slurm/<site>/` to `<run_dir>/<fleet>/` on the cluster.
2. On the login node, submit the directory's job, and wait for its endpoint file:

   ```bash
   srtctl apply -f <run_dir>/<fleet>/directory.yaml --json
   cat <run_dir>/<fleet>/directory.<job id>.endpoint
   ```

3. Submit each pool's job, in any order, and wait for its health:

   ```bash
   srtctl apply -f <run_dir>/<fleet>/<pool>/pool.yaml --json
   curl -s http://<hub node>:1337/v1/ext/gdl-pool/health   # 200 once the pool is ready
   ```

Pyxis pulls a registry's image named as `<registry host>#<path>/<repository>@<digest>`, and the recipes name each image that way.

## Check the recipes with srt-slurm

```bash
bash scripts/slurm-dry-run.sh            # every pinned recipe
bash scripts/slurm-dry-run.sh slurm/     # the recipes a render wrote
```

The script runs `srtctl dry-run` on each recipe, at the pinned srt-slurm commit, fetched through uv, with a placeholder `srtslurm.yaml`. A recipe srt-slurm refuses fails it. `srtctl dry-run` submits nothing. The pinned recipes are `crates/gdl-gen/tests/fixtures/slurm/out`.
