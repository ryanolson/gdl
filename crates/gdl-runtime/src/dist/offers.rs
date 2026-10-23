//! Which of a segment's hops bring it offers: forward hand-offs from
//! another class, which a draining cell's entry instance declines.

use gdl_model::ClassBoundary;

/// For each of `hops` hops, whether its forward hand-offs that reach this
/// segment are offers: the hop crosses one of `boundaries` at its inner
/// end, and `inner_here` says this segment holds that end. The hop's
/// other stages are all in the class it leaves (the boundary rule), so
/// every hand-off of it that reaches the segment comes from another class.
/// One pass over the boundaries, so that a graph with many of them costs
/// in proportion to its size.
pub(crate) fn offered_hops<'a>(
    boundaries: impl IntoIterator<Item = &'a ClassBoundary>,
    hops: usize,
    inner_here: impl Fn(usize) -> bool,
) -> Vec<bool> {
    let mut offered = vec![false; hops];
    for boundary in boundaries {
        if let Some(offer) = offered.get_mut(boundary.hop) {
            *offer = inner_here(boundary.hop);
        }
    }
    offered
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use gdl_model::ClassBoundary;

    use super::offered_hops;

    /// Boundaries at every other one of `hops` hops.
    fn boundaries(hops: usize) -> Vec<ClassBoundary> {
        (0..hops)
            .step_by(2)
            .map(|hop| ClassBoundary {
                hop,
                from: "global".into(),
                into: "model_x".into(),
            })
            .collect()
    }

    /// How many boundaries classifying `hops` hops reads, and what it
    /// finds.
    fn classified(hops: usize) -> (usize, Vec<bool>) {
        let boundaries = boundaries(hops);
        let read = Cell::new(0);
        let counted = boundaries.iter().inspect(|_| read.set(read.get() + 1));
        let offered = offered_hops(counted, hops, |hop| hop % 4 == 0);
        (read.get(), offered)
    }

    /// Each hop that crosses a boundary at an inner end this segment holds
    /// is an offer, and no other; classifying them reads each boundary a
    /// bounded number of times, so twice the hops cost twice the reads,
    /// not four times.
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
