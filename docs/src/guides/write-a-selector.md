# Write a selector

A selector chooses the instance a request goes to when it crosses into another segment. This guide writes one, registers it, and names it in a placement. [Routing, selectors, and offers](../concepts/routing.md) explains what a selector sees and when it runs. Try gdl's own `affinity` and `least_loaded` first: they need no code.

## 1. Implement `gdl::Selector`

Pick the `Head` type. Use the head type of the edge that crosses (`H` for a streamed edge `Streamed<H, T>`) to read the head as a value. Use `gdl::Projected<P>` to read only some of its fields ([Write a cell policy](write-a-cell-policy.md#1-read-only-what-you-route-by)). Use `gdl::Encoded` to read only its bytes, with no decode, on any edge.

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

## 3. Register it

In a segment binary, implement `gdl::Build` for the selector, as for a node, and register it in the hand-written `SegmentBinary`'s `build` with

`builder.selector::<ByLength>("by_length").await?`. The `Builder` builds it from the segment's `[selector.by_length]` table, registers its metrics with `dynamo_endpoint = "by_length"`, and waits for its `ready` before the instance announces that it is loaded. A generated binary routes only with gdl's own selectors (see [Known limitations](../operations/limitations.md)).

In an in-process deployment, add it with `config.selectors.add("by_length", ByLength)?` on a `RuntimeConfig`. Here it needs no `Build`. A name is lowercase letters and digits in words joined by single `_`s, starts with a letter, has at most 64 characters, and is not `round_robin` or `random`.

## 4. Name it in the placement

Write `Engine = { selector = "by_length" }` in the placement's `[routing]` table. The segment refuses to start when the name resolves to no selector, or when `Head` does not match the edge. A `[selector.<name>]` table for a selector that the binary does not register, and gdl does not have, is refused too. [Segment settings](../operations/configuration.md) covers the `[selector.*]` tables.
