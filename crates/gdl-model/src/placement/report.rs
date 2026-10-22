use std::fmt;

use super::HopLayout;

/// Aggregate segment crossings and anchors for one request through each
/// root and, when the placement has classes, the WAN round trips its class
/// boundaries add to a request that spills to another region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossingReport {
    pub(super) roots: usize,
    pub(super) hops: Vec<HopCount>,
    // Without classes the report reads as it did before classes existed,
    // so that the generated `crossings.txt` of every such spec is unchanged.
    pub(super) classed: bool,
}

impl CrossingReport {
    /// Crossings for one request through each root, over all hops.
    pub fn crossings(&self) -> usize {
        self.hops.iter().map(|hop| hop.crossings).sum()
    }

    /// Anchors for one request through each root, over all hops.
    pub fn anchors(&self) -> usize {
        self.hops.iter().map(|hop| hop.anchors).sum()
    }

    /// WAN round trips for one request through each root, over all hops,
    /// when every class boundary it crosses spills to another region. In
    /// one region a boundary adds none. Zero without classes.
    pub fn wan_round_trips(&self) -> usize {
        self.hops.iter().map(|hop| hop.wan_round_trips).sum()
    }
}

impl fmt::Display for CrossingReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, hop) in self.hops.iter().enumerate() {
            write!(
                f,
                "hop {index}: {} crossings, {} anchors",
                hop.crossings, hop.anchors
            )?;
            if self.classed {
                write!(f, ", {} WAN round trips", hop.wan_round_trips)?;
                if let Some((from, into)) = &hop.boundary {
                    write!(f, " ({from} -> {into})")?;
                }
            }
            writeln!(f)?;
        }
        write!(
            f,
            "{}: {} crossings, {} anchors",
            if self.roots == 1 {
                "per request"
            } else {
                "one request per root"
            },
            self.crossings(),
            self.anchors()
        )?;
        if self.classed {
            write!(f, ", {} WAN round trips", self.wan_round_trips())?;
        }
        Ok(())
    }
}

/// One hop's line of a [`CrossingReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HopCount {
    pub(super) crossings: usize,
    pub(super) anchors: usize,
    pub(super) wan_round_trips: usize,
    /// The classes a boundary hop crosses from and into.
    pub(super) boundary: Option<(String, String)>,
}

/// The WAN round trips one request adds at a class boundary when the cell
/// it is offered to is in another region, between warm peers. The
/// `gdl.fwd` goes out and the inner end's output opens the outer end's
/// ticket back, with no attach round trip. A streamed request's items wait
/// for the acknowledgement that brings back the inner end's input anchor,
/// so it takes a second. The boundary rule makes the edge into the inner
/// end the hop's only crossing out of the outer end's segment. `_hello` and
/// a cold connection's handshake are paid once per pair of instances, not
/// per request, and are not counted.
pub(super) fn spill_round_trips(hop: &HopLayout) -> usize {
    if hop.request_streams.last() == Some(&true) {
        2
    } else {
        1
    }
}
