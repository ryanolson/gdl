# The source reader: what a crate's shape costs it

Review `m19-r12` (rows M2 and S1) found the reader's memory and time growing with the product of two lengths in a source file, not their sum. The composer's import child bounds that since pass 12. The owner then asked for the reader itself to be fixed (ruling 103). These are the measurements before and after that fix.

Measured on 2026-10-20 on the macOS workstation (Apple silicon), with the release `cargo gdl extract` on generated crates. "rss" is the peak resident set of the process tree (`/usr/bin/time -l`), and "time" is its wall time. About 37 MiB and 0.3 s of each are cargo metadata and the process itself. "Before" is `ea0670a`; "after" is the commit that adds this file.

```bash
cargo build --release -p gdl-gen
python3 gen.py <dir> <shape> <a> <b>   # writes <dir>/<shape>-<a>-<b>, a crate with one src/lib.rs
/usr/bin/time -l target/release/cargo-gdl gdl extract <dir>/<shape>-<a>-<b> --target-dir <scratch>
```

`gen.py`, run with `GDL_REPO` set to the checkout:

```python
import os, sys
repo = os.environ["GDL_REPO"]  # the checkout whose gdl-message the crates use
root, shape, a, b = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
d = os.path.join(root, f"{shape}-{a}-{b}")
os.makedirs(os.path.join(d, "src"), exist_ok=True)
with open(os.path.join(d, "Cargo.toml"), "w") as f:
    f.write(f'[package]\nname = "probe"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n[dependencies]\n'
            f'gdl-message = {{ path = "{repo}/crates/gdl-message" }}\nserde = {{ version = "1", features = ["derive"] }}\n')
src = []
if shape == "attr":      # one `use` group of b names, carrying a doc attribute of a bytes
    src.append(f'#[doc = "{"x" * a}"]\npub use self::inner::{{{", ".join(f"n{i}" for i in range(b))}}};\npub mod inner {{}}\n')
elif shape == "prefix":  # one `use` group of b names under a prefix of a segments
    src.append(f'pub use {"::".join(f"p{i}" for i in range(a))}::{{{", ".join(f"n{i}" for i in range(b))}}};\n')
elif shape == "derived":
    src.append("use serde::{Serialize, Deserialize};\n")
    for i in range(a):
        src.append(f"#[derive(Serialize, Deserialize, gdl_message::Message)]\npub struct M{i} {{ pub v: u32 }}\nimpl Default for M{i} {{ fn default() -> Self {{ todo!() }} }}\n")
elif shape == "msgs":    # a messages, each with a hand-written Deserialize, plus b plain impls
    src.append("use serde::{Serialize, Deserialize};\n")
    for i in range(a):
        src.append(f"#[derive(Serialize, gdl_message::Message)]\npub struct M{i} {{ pub v: u32 }}\n"
                   f"impl<'de> Deserialize<'de> for M{i} {{ fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {{ todo!() }} }}\n")
    for i in range(b):
        src.append(f"pub struct P{i};\nimpl P{i} {{}}\n")
open(os.path.join(d, "src/lib.rs"), "w").write("".join(src))
print(d, os.path.getsize(os.path.join(d, "src/lib.rs")))
```

The shapes it writes:

- `attr a b`: `#[doc = "<a bytes>"] pub use self::inner::{n0, ..., n<b>};`.
- `prefix a b`: `pub use p0::...::p<a>::{n0, ..., n<b>};`.
- `msgs a 0`: `a` messages, each `#[derive(Serialize, gdl_message::Message)]` with a hand-written `impl Deserialize`, as generated serde code writes it.
- `derived a 0`: `a` messages that derive both serde traits, each with an `impl Default`. This is the control: the wire check never walks impls for them.

| Shape | File | Before: rss, time | After: rss, time |
|---|---|---|---|
| `attr 10000 10000` | 77 KiB | 123 MiB, 3.1 s | not run |
| `attr 40000 10000` | 106 KiB | 490 MiB, 3.1 s | not run |
| `attr 140000 20000` | 282 KiB | 2,902 MiB, 13.0 s | 37 MiB, 0.35 s |
| `prefix 5000 5000` | 66 KiB | 1,027 MiB, 2.5 s | not run |
| `prefix 10000 10000` | 134 KiB | 3,922 MiB, 9.2 s | 37 MiB, 0.07 s |
| `msgs 1000 0` | 197 KiB | 38 MiB, 1.3 s | 37 MiB, 0.07 s |
| `msgs 2000 0` | 396 KiB | 59 MiB, 5.6 s | 59 MiB, 0.09 s |
| `msgs 8000 0` | 1.6 MiB | not run | 227 MiB, 0.83 s |
| `derived 8000 0` | 1.1 MiB | 142 MiB, 0.54 s | 144 MiB, 0.15 s |

Before the fix:

- Each name a `use` binds copied the item's attributes and the path before its braces.
- Each module copied its parent's path.
- Each name's location re-tokenized the whole `use` item, because syn's `span` builds the item's tokens.
- The wire check of each message, and the role check of each node, walked and resolved every impl in the crate.
- A marker written as a single name scanned every `use` of its module for each name.

After the fix:

- A `use` item's attributes and path prefix are shared by its names.
- A module stores only its own name.
- An item's location is computed once.
- The crate's serde and role impls are resolved once and indexed by the type they are for.
- A module indexes its types and bound names by name.

The remaining cost is linear. A crate of messages peaks at about 120 times its source size above the baseline, which is the parsed file plus what the reader keeps. The 16 MiB extraction budget therefore bounds the reader near 2 GB.

`crates/gdl-extract/tests/cost.rs` holds each shape at two sizes. It asserts that doubling the source at most about doubles the peak memory and the allocations, or, for the marker scan, the time. Every assertion fails on the code before the fix: memory or allocations grew 3.9 to 4.0 times when the source doubled, and time 3.7 times.
