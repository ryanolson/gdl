use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;

use super::calls::Site;
use super::{ClassBoundary, HopLayout};

#[cfg(test)]
mod tests;

/// Aggregate segment crossings and anchors for one request through each
/// root and, when the placement has classes, the WAN round trips its class
/// boundaries add to a request that spills to another region. Apart from
/// the hops, each caller's calls: the services it calls in another
/// segment, and how many it calls in its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossingReport {
    pub(super) roots: usize,
    pub(super) hops: Vec<HopCount>,
    // Without classes the report reads as it did before classes existed,
    // so that the generated `crossings.txt` of every such spec is unchanged.
    pub(super) classed: bool,
    // A graph without callers writes no call line, for the same reason.
    pub(super) calls: Vec<CallLine>,
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

    /// The `calls` entries, over every copy of every caller, that call a
    /// copy of the service in another segment. A node may make any number
    /// of calls per request, so this counts what may be called, not calls
    /// per request. A remote call is not a crossing.
    pub fn remote_calls(&self) -> usize {
        self.calls.iter().map(|line| line.remote.len()).sum()
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
        for line in &self.calls {
            write!(
                f,
                "\ncalls from {} in {}: {} remote",
                line.caller,
                line.segment,
                line.remote.len()
            )?;
            if !line.remote.is_empty() {
                let called: Vec<String> = line
                    .remote
                    .iter()
                    .map(|(service, to)| format!("{service} in {to}"))
                    .collect();
                write!(f, " ({})", called.join(", "))?;
            }
            write!(f, ", {} local", line.local)?;
        }
        Ok(())
    }
}

/// One caller's line of a [`CrossingReport`], for one segment it is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CallLine {
    caller: String,
    segment: String,
    /// Each service it calls in another segment, with that segment, in the
    /// order its `calls` lists them.
    remote: Vec<(String, String)>,
    /// How many services it calls in its own segment.
    local: usize,
}

/// A line for each caller and each segment it is in, in name order.
pub(super) fn call_lines(sites: &[Site]) -> Vec<CallLine> {
    let mut lines: BTreeMap<(&str, &str), CallLine> = BTreeMap::new();
    for site in sites {
        let line = lines
            .entry((&site.caller, &site.segment))
            .or_insert_with(|| CallLine {
                caller: site.caller.clone(),
                segment: site.segment.clone(),
                remote: Vec::new(),
                local: 0,
            });
        if site.remote() {
            line.remote.push((site.service.clone(), site.to.clone()));
        } else {
            line.local += 1;
        }
    }
    lines.into_values().collect()
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

/// Each hop's line: its crossings and anchors and, for a boundary hop, its
/// WAN round trips and classes. Each boundary is read once, by its hop's
/// index, so the report grows with the hops. Generic over how a boundary
/// is held so that a test can count every read: a search for each hop's
/// boundary allocates nothing, so only a count of reads tells it from this.
pub(super) fn hop_lines<B: Borrow<ClassBoundary>>(
    hops: &[HopLayout],
    boundaries: &[B],
) -> Vec<HopCount> {
    let mut lines: Vec<HopCount> = hops
        .iter()
        .map(|hop| HopCount {
            crossings: hop.crossings(),
            anchors: hop.anchors(),
            wan_round_trips: 0,
            boundary: None,
        })
        .collect();
    for boundary in boundaries {
        let boundary = boundary.borrow();
        let line = &mut lines[boundary.hop];
        line.wan_round_trips = spill_round_trips(&hops[boundary.hop]);
        line.boundary = Some((boundary.from.clone(), boundary.into.clone()));
    }
    lines
}

/// The WAN round trips one request adds at a class boundary when the pool
/// it is offered to is in another region, between warm peers. The
/// `gdl.fwd` goes out and the inner end's output opens the outer end's
/// ticket back, with no attach round trip. A streamed request's items wait
/// for the acknowledgement that brings back the inner end's input anchor,
/// so it takes a second. The boundary rule makes the edge into the inner
/// end the hop's only crossing out of the outer end's segment. `_hello` and
/// a cold connection's handshake are paid once per pair of instances, not
/// per request, and are not counted.
fn spill_round_trips(hop: &HopLayout) -> usize {
    if hop.request_streams.last() == Some(&true) {
        2
    } else {
        1
    }
}
