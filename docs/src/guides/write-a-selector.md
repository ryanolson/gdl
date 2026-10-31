# Write a selector

A selector chooses the instance a request goes to when it crosses into another segment. This guide writes one, binds it in the spec or registers it, and names it in a placement. [Routing, selectors, and offers](../concepts/routing.md) explains what a selector sees and when it runs. Try gdl's own `affinity` and `least_loaded` first: they need no code.

## 1. Implement `gdl::Selector`

Pick the `Head` type. Use the head type of the edge that crosses (`H` for a streamed edge `Streamed<H, T>`) to read the head as a value. Use `gdl::Projected<P>` to read only some of its fields ([Write a pool policy](write-a-pool-policy.md#1-read-only-what-you-route-by)). Use `gdl::Encoded` to read only its bytes, with no decode, on any edge.

```rust
use gdl::{Choice, Instances, Request, Result, Selector};

/// Short prompts go to the first instance, long ones to the last.
struct ByLength;

impl Selector for ByLength {
    type Head = Prompt;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Prompt>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        let short = request.head().tokens < 512;
        let declined = request.declined();
        let mut open = instances.iter().filter(|i| !declined.contains(&i.id()));
        let found = if short { open.next() } else { open.last() };
        let chosen = found.unwrap_or_else(|| instances.get(0).expect("never empty"));
        let class = if short { "short" } else { "long" };
        Ok(Choice::new(chosen).with_meta("length_class", class))
    }
}
```

- Return a `Choice` made from an `Instance` of `instances`. Key your own state by `instance.id()`, which is the same from every sender.
- Read `request.declined()` after a decline. Prefer an instance that has not declined, or the failover bound can go to the same instance each time.
- Return `Error::routing(...)` when you cannot choose, and an error of the kind `Overloaded` (`Error::new(ErrorKind::Overloaded, ...)`) when no instance has room.

## 2. Hold state with a lease

To count what is in flight, add to your count when you choose, and return a lease that gives it back on drop:

```rust
struct Release(Arc<AtomicUsize>);
impl Drop for Release {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::Relaxed); }
}
// in select: count.fetch_add(1, Ordering::Relaxed);
Ok(Choice::new(chosen).with_lease(Release(Arc::clone(&count))))
```

The runtime drops the lease when the sending segment stops holding the request, and also when it asks again after a decline or a membership change. Take no global lock in `select`: it runs for every hand-off.

### Read the pools' properties

A selector can read the properties of the pools its segment follows, as a node does ([What a node reads](../concepts/pool-routing.md#what-a-node-reads)). Name each kind it reads in its `NEEDS`, and register each in its build:

```rust
impl Selector for ByShard {
    type Head = Encoded;
    const NEEDS: &'static [&'static str] = &["shard"];
    // select ...
}

impl Build for ByShard {
    // ...
    async fn build(_: ByShardSettings, (): ()) -> gdl::Result<Self> {
        let followed = gdl::FollowedPools::current();
        followed.register(&SHARD)?;
        Ok(ByShard { followed })
    }
}
```

- `cargo gdl check`, `generate`, and `build` read `NEEDS` from the source, as a list of string literals. Each kind must be declared in the graph, and supplied by the class of each crossing the selector routes: an entry segment of the class hosts a node that provides it, or `[pools.<class>] provides` names it ([Define a graph](define-a-graph.md#declare-properties)). The error names the kind, the selector, and the class.
- The selector reads the pools of the class it sends into. So a selector on a crossing inside a class, or in a placement without classes, reads no pools, and a `NEEDS` there is refused.
- The selector's build may register only the kinds its `NEEDS` names, as a pool policy's `properties` may. Its segment's nodes' needs do not count.
- A selector registered in code is held to the same supply when its segment is deployed, and the segment does not start otherwise.
- `NEEDS` enters no hash. By default a selector needs none.

## 3. Bind it in the spec

For a generated binary, implement `gdl::Build` too: the segment builds the selector from its settings, as it builds a node.

```rust
use gdl::Build;

#[derive(serde::Deserialize)]
struct ByLengthSettings {}

impl Build for ByLength {
    type Config = ByLengthSettings;
    type Metrics = ();

    async fn build(_: ByLengthSettings, (): ()) -> gdl::Result<Self> {
        Ok(ByLength)
    }
}
```

Then name its crate and type in the spec, under the name the placement uses:

```toml
[selector.by_length]
crate = "my-routers"
path = "routers"
type = "my_routers::ByLength"
```

- `cargo gdl check` reads the crate and refuses a type that the crate does not offer as a selector. Write the type from the crate's root, as `my_routers::ByLength`, and make it reachable there.
- `cargo gdl check` compares the selector's `Head` with the request of each crossing that names it, as the sending segment does when it starts. The rules are a pool policy's ([Write a pool policy](write-a-pool-policy.md#4-bind-it-in-the-spec)). A spec with a `[contract]` crate is not checked this way.
- Each segment that sends through a crossing named `by_length` in `[routing]` depends on the crate, and its `build` registers the selector with one `builder.selector::<my_routers::ByLength>("by_length")` line. No other segment depends on the crate. The emitted line names the type, so the binary links it.
- That segment builds the selector from the `[selector.by_length]` table of its settings file, or `GDL_SELECTOR__BY_LENGTH__<KEY>` variables, registers its metrics with `dynamo_endpoint = "by_length"`, and waits for its `ready` before the instance announces that it is loaded.
- The name cannot be gdl's own `affinity` or `least_loaded`, nor the name of a node, an entry adapter, a `[config]` key, or a bound pool policy: the segment builds them all under their names.
- One crate can hold a pool policy and a selector. The segment that uses both depends on it once.
- The spec can name the crate by `version`, or by `git` with `rev` (and `lib` when the library has another name), in place of `path`, as for a pool policy.
- A spec that binds no selector adds no dependency and no registration to any segment.

## 4. Or register it in code

In a hand-written `SegmentBinary`, call `builder.selector::<ByLength>("by_length").await?` in `build`. The `Builder` builds it from the segment's `[selector.by_length]` table, as a generated segment does.

In an in-process deployment, add it with `config.selectors.add("by_length", ByLength)?` on a `RuntimeConfig`. A program that deploys a generated segment through its library's `start` does this too: only the segment's binary registers a bound selector. Here it needs no `Build`. A name is lowercase letters and digits in words joined by single `_`s, starts with a letter, has at most 64 characters, and is not `round_robin` or `random`.

## 5. Name it in the placement

Write `Engine = { selector = "by_length" }` in the placement's `[routing]` table. The segment refuses to start when the name resolves to no selector, or when `Head` does not match the edge. A `[selector.<name>]` settings table for a selector that the binary does not register, and gdl does not have, is refused too. [Segment settings](../operations/configuration.md) covers the `[selector.*]` tables.
