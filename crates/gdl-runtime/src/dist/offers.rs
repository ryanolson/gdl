//! Which of a segment's hops bring it offers: forward hand-offs from
//! another class, which a draining cell's entry instance declines.

use gdl_model::ClassBoundary;

/// A class boundary, as the classification reads it: the hop it falls on.
pub(crate) trait Boundary {
    /// The index of the hop the boundary falls on.
    fn hop(&self) -> usize;
}

impl Boundary for ClassBoundary {
    fn hop(&self) -> usize {
        self.hop
    }
}

/// For each of `hops` hops, whether its forward hand-offs that reach this
/// segment are offers: the hop crosses one of `boundaries` at its inner
/// end, and `inner_here` says this segment holds that end. The hop's
/// other stages are all in the class it leaves (the boundary rule), so
/// every hand-off of it that reaches the segment comes from another class.
/// One pass over the boundaries, so that a graph with many of them costs
/// in proportion to its size.
pub(crate) fn offered_hops<'a, B: Boundary + 'a>(
    boundaries: impl IntoIterator<Item = &'a B>,
    hops: usize,
    inner_here: impl Fn(usize) -> bool,
) -> Vec<bool> {
    let mut offered = vec![false; hops];
    for boundary in boundaries {
        let hop = boundary.hop();
        if let Some(offer) = offered.get_mut(hop) {
            *offer = inner_here(hop);
        }
    }
    offered
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{Boundary, offered_hops};

    /// A boundary that counts each time its hop is read.
    struct Counted<'a> {
        hop: usize,
        reads: &'a Cell<usize>,
    }

    impl Boundary for Counted<'_> {
        fn hop(&self) -> usize {
            self.reads.set(self.reads.get() + 1);
            self.hop
        }
    }

    /// How many reads of a boundary classifying `hops` hops takes, with a
    /// boundary at every other hop, and what it finds.
    fn classified(hops: usize) -> (usize, Vec<bool>) {
        let reads = Cell::new(0);
        let boundaries: Vec<Counted<'_>> = (0..hops)
            .step_by(2)
            .map(|hop| Counted { hop, reads: &reads })
            .collect();
        let offered = offered_hops(&boundaries, hops, |hop| hop % 4 == 0);
        (reads.get(), offered)
    }

    /// Each hop that crosses a boundary at an inner end this segment holds
    /// is an offer, and no other; classifying them reads each boundary a
    /// bounded number of times, however it holds them, so twice the hops
    /// cost twice the reads, not four times.
    #[test]
    fn classifying_offers_reads_each_boundary_once() {
        let (_, offered) = classified(8);
        assert_eq!(
            offered,
            [true, false, false, false, true, false, false, false]
        );
        let (small, _) = classified(512);
        let (large, _) = classified(1024);
        assert!(
            large <= small * 3,
            "{small} reads for 512 hops, {large} for 1024"
        );
    }
}
