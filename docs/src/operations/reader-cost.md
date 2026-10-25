# Reader cost

The source reader (`cargo gdl extract`) reads a crate's source without building it. This chapter reports what a crate's shape costs the reader in memory and time. [Catalogue design](../development/catalogue-design.md) says how the reader keeps its cost linear.

Without care the cost grew with the product of two lengths in a source file, not their sum. After the fix it grows with the source: a crate of messages peaks at about 120 times its source size above the baseline, which is the parsed file plus what the reader keeps. The 16 MiB extraction budget therefore bounds what the reader stores near 2 GB.

**Setup.** Apple silicon, macOS, 2026-10-20, release `cargo gdl extract` on generated crates. Before is `ea0670a`, after is `005b52b`. "rss" is the peak resident set of the process tree (`/usr/bin/time -l`) and "time" is wall time. About 37 MiB and 0.3 s of each is cargo metadata and the process itself.

```bash
cargo build --release -p gdl-gen
python3 gen.py <dir> <shape> <a> <b>   # GDL_REPO is the checkout whose gdl-message the crates use
/usr/bin/time -l target/release/cargo-gdl gdl extract <dir>/<shape>-<a>-<b> --target-dir <scratch>
```

| Shape | File | Before: rss, time | After: rss, time |
|---|---|---|---|
| `attr 140000 20000` | 282 KiB | 2,902 MiB, 13.0 s | 37 MiB, 0.35 s |
| `prefix 10000 10000` | 134 KiB | 3,922 MiB, 9.2 s | 37 MiB, 0.07 s |
| `msgs 2000 0` | 396 KiB | 59 MiB, 5.6 s | 59 MiB, 0.09 s |
| `msgs 8000 0` | 1.6 MiB | not run | 227 MiB, 0.83 s |
| `derived 8000 0` | 1.1 MiB | 142 MiB, 0.54 s | 144 MiB, 0.15 s |

The shapes: `attr a b` is one `use` group of `b` names with a doc attribute of `a` bytes. `prefix a b` is one group of `b` names under a prefix of `a` segments. `msgs a b` is `a` messages with a hand-written `Deserialize`, as generated serde code writes it, and `b` plain impls. `derived a 0` is the control: messages that derive both serde traits, which the wire check never walks impls for.

```python
import os, sys
repo = os.environ["GDL_REPO"]
root, shape, a, b = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
d = os.path.join(root, f"{shape}-{a}-{b}"); os.makedirs(d + "/src", exist_ok=True)
open(d + "/Cargo.toml", "w").write('[package]\nname = "probe"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n[dependencies]\n'
    f'gdl-message = {{ path = "{repo}/crates/gdl-message" }}\nserde = {{ version = "1", features = ["derive"] }}\n')
names = ", ".join(f"n{i}" for i in range(b)); s = "use serde::{Serialize, Deserialize};\n"
if shape == "attr":   s = f'#[doc = "{"x" * a}"]\npub use self::inner::{{{names}}};\npub mod inner {{}}\n'
if shape == "prefix": s = f'pub use {"::".join(f"p{i}" for i in range(a))}::{{{names}}};\n'
if shape == "derived":
    s += "".join(f"#[derive(Serialize, Deserialize, gdl_message::Message)]\npub struct M{i} {{ pub v: u32 }}\nimpl Default for M{i} {{ fn default() -> Self {{ todo!() }} }}\n" for i in range(a))
if shape == "msgs":
    s += "".join(f"#[derive(Serialize, gdl_message::Message)]\npub struct M{i} {{ pub v: u32 }}\nimpl<'de> Deserialize<'de> for M{i} {{ fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {{ todo!() }} }}\n" for i in range(a))
    s += "".join(f"pub struct P{i};\nimpl P{i} {{}}\n" for i in range(b))
open(d + "/src/lib.rs", "w").write(s)
```

**What was quadratic.**
- Each name a `use` binds copied the item's attributes and the path before its braces.
- Each module copied its parent's path.
- Each name's location re-tokenized the whole `use` item, because syn's `span` builds the item's tokens.
- The wire check of each message, and the role check of each node, walked and resolved every impl in the crate.
- A marker written as a single name scanned every `use` of its module for each name.

`crates/gdl-extract/tests/cost.rs` holds each shape at two sizes, plus the nested-path shapes below. It asserts that doubling the source at most about doubles the peak memory and the allocations, or, for the marker scan, the time. On the code before the fix, doubling grew memory or allocations 3.9 to 4.0 times, and time 3.7 times. The nested shapes were: paths inside braces under a long prefix, groups nested in groups, deep inline modules with long names, and one `use` item with many attributes binding many names.

**Residual costs**, bounded by the import child's limits and not by the budget. A resolution repeats, per query, the chain it follows: N resolutions through a K-segment path, or past K globs, cost N times K. Output whose size is itself the product also remains, such as a full path for each message of a deep chain.
