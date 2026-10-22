use std::borrow::Borrow;
use std::cell::Cell;

use super::hop_lines;
use crate::placement::{ClassBoundary, HopLayout, Run};

/// A boundary that counts each time it is read.
struct Counted<'a> {
    boundary: ClassBoundary,
    reads: &'a Cell<usize>,
}

impl Borrow<ClassBoundary> for Counted<'_> {
    fn borrow(&self) -> &ClassBoundary {
        self.reads.set(self.reads.get() + 1);
        &self.boundary
    }
}

/// `n` hops between two segments, each a class boundary.
fn hops(n: usize) -> Vec<HopLayout> {
    let run = |segment: &str, start| Run {
        segment: segment.into(),
        start,
        stages: vec![segment.into()],
    };
    (0..n)
        .map(|_| HopLayout {
            local: false,
            forward_runs: vec![run("a", 0), run("b", 1)],
            backward_runs: vec![run("b", 0), run("a", 1)],
            provisioning: vec![],
            request_streams: vec![false],
        })
        .collect()
}

/// The boundary reads one report of `n` boundary hops makes.
fn reads(n: usize) -> usize {
    let reads = Cell::new(0);
    let boundaries: Vec<Counted> = (0..n)
        .map(|hop| Counted {
            boundary: ClassBoundary {
                hop,
                from: "global".into(),
                into: "model_x".into(),
            },
            reads: &reads,
        })
        .collect();
    let lines = hop_lines(&hops(n), &boundaries);
    assert!(lines.iter().all(|line| line.wan_round_trips == 1));
    reads.get()
}

/// A search for each hop's boundary allocates nothing, so the allocation
/// count of `tests/cost.rs` cannot see it; the reads can.
#[test]
fn the_report_reads_each_boundary_a_bounded_number_of_times() {
    let (small, large) = (reads(256), reads(512));
    assert!(large <= 3 * small, "reads {small} then {large}");
    assert!(large <= 2 * 512, "{large} reads for 512 boundaries");
}
