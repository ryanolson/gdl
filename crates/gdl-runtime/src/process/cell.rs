//! Where a launch's stop meets its cell's drain: when the drain begins the
//! stop, what it changes about the drain, and what the instance announces
//! at its end; and what an entry instance's retirement waits for across
//! its cell's boundary. An instance outside a cell, under a roster
//! directory, or in a binary without hub discovery, takes no part in
//! either, and every answer here is the one its stop has without a cell.

#[cfg(feature = "hub")]
use super::{DRAIN_POLL, Peers};
use super::{Drains, Launch};

impl Launch {
    /// Resolves when the cell's drain begins this instance's stop; never
    /// for an instance that takes no part in one.
    pub(super) async fn cell_drain_stops(&self) {
        #[cfg(feature = "hub")]
        if let (Peers::Hub(session), Some(cell)) = (&self.peers, &self.cell_drain) {
            return cell.stops(session).await;
        }
        std::future::pending().await
    }

    /// Whether the stop the cell's drain begins retires the instance.
    pub(super) fn cell_drain_retires(&self) -> bool {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain {
            return cell.retires();
        }
        true
    }

    /// The segments whose instances are this one's drain sources: while
    /// its cell drains, the cell's entry segments, for an entry instance
    /// too; otherwise those hosting a source that can reach it. `None` when
    /// their drain cannot be seen from here, for an instance that a source
    /// of another class reaches: the drain of the sources' requests (B)
    /// then never holds, and the instance waits for its senders, and an
    /// entry instance for its parent too (A).
    pub(super) fn drain_sources(&self) -> Option<Vec<String>> {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain {
            if let Some(sources) = cell.sources() {
                return Some(sources.to_vec());
            }
            if cell.blind() {
                return None;
            }
        }
        Some(self.reaching.clone())
    }

    /// The instance retires now: from here an entry instance in a cell
    /// counts its wait for the cells that follow its cell.
    pub(super) fn cell_retires(&mut self) {
        #[cfg(feature = "hub")]
        if let Some(cell) = &mut self.cell_drain {
            cell.retire();
        }
    }

    /// Whether the cells that follow this instance's cell may be taken to
    /// avoid it, asked by (A) at each poll: for an entry instance that
    /// retired, once its hub says they do, or, while its cell drains, that
    /// none follows it, or the bound has passed since it retired and since
    /// it learned of a drain; for any other, always. What it reads
    /// is taken now, so that the question borrows nothing of the launch.
    #[cfg(feature = "hub")]
    pub(super) fn cell_parent(&self) -> impl Fn() -> bool + use<> {
        let parent = self.cell_drain.as_ref().and_then(|cell| cell.parent());
        move || parent.as_ref().is_none_or(crate::dist::Parent::avoids)
    }

    /// Whether this instance, a retired entry instance, now declines
    /// offers because of a drain of its cell that it learned of while it
    /// waits ([`crate::dist::Parent::declines`]). It then says `drained`
    /// once it holds no request, as an entry instance that the drain
    /// stopped does: every instance of the cell waits for that, and a
    /// segment that sends back to the entry would otherwise wait for the
    /// entry while the entry waits for it to leave. Always `false` for any
    /// other instance. What it reads is taken now, so that the question
    /// borrows nothing of the launch.
    #[cfg(feature = "hub")]
    pub(super) fn cell_declines(&self) -> impl Fn() -> bool + use<> {
        let parent = self.cell_drain.as_ref().and_then(|cell| cell.parent());
        move || parent.as_ref().is_some_and(crate::dist::Parent::declines)
    }

    /// For a retired entry instance, decline offers from `segment` from
    /// other cells once a drain of its cell it learned of while it waits
    /// reaches its bound, or no cell follows the cell; then, and for any
    /// other instance, whose stop decided that as it began, never resolve.
    /// It runs beside the drain's waiting rather than between its
    /// questions to the hub, which may stall. What it reads is taken now,
    /// so that it borrows nothing of the launch.
    pub(super) fn cell_declining<'a>(
        &self,
        segment: &'a dyn Drains,
    ) -> impl Future<Output = ()> + use<'a> {
        #[cfg(feature = "hub")]
        let parent = self.cell_drain.as_ref().and_then(|cell| cell.parent());
        #[cfg(not(feature = "hub"))]
        let _ = segment;
        async move {
            #[cfg(feature = "hub")]
            if let Some(parent) = parent {
                parent.declining().await;
                segment.decline_offers();
            }
            std::future::pending::<()>().await;
        }
    }

    /// For an entry instance of a draining cell, decline offers once its
    /// bound has passed or no other cell follows the cell, or at
    /// `deadline`; for any other, nothing. What it reads is taken now, so
    /// that the wait borrows nothing of the launch while the stop goes on
    /// beside it.
    pub(super) fn cell_drain_declines<'a>(
        &self,
        segment: &'a dyn Drains,
        deadline: tokio::time::Instant,
    ) -> impl Future<Output = ()> + use<'a> {
        #[cfg(feature = "hub")]
        let declining = self.cell_drain.as_ref().and_then(|cell| cell.declining());
        #[cfg(not(feature = "hub"))]
        let declining: Option<std::future::Ready<()>> = None;
        async move {
            if let Some(declining) = declining {
                tokio::select! {
                    () = declining => {}
                    () = tokio::time::sleep_until(deadline) => {}
                }
                segment.decline_offers();
            }
        }
    }

    /// For an entry instance of a draining cell: wait until its bound has
    /// passed or no other cell follows the cell, or until `deadline`,
    /// decline offers, wait until it holds no
    /// request, so that every request it took has been answered, and
    /// announce that it has drained, retried until `deadline`; with `hub`
    /// false (its lease is lost), nothing is announced. Meanwhile it
    /// announces that it detached once no request it holds can send work
    /// into the cell ([`Launch::cell_detaches`]). Any other instance goes
    /// on at once.
    pub(super) async fn cell_drain_settles(
        &mut self,
        segment: &dyn Drains,
        deadline: tokio::time::Instant,
        hub: bool,
    ) {
        #[cfg(feature = "hub")]
        if let Some(cell) = &self.cell_drain
            && cell.announces()
        {
            cell.accepting(deadline).await;
            segment.decline_offers();
            loop {
                self.cell_detaches(segment, hub, false).await;
                if segment.admitted() == 0 || tokio::time::Instant::now() + DRAIN_POLL > deadline {
                    break;
                }
                tokio::time::sleep(DRAIN_POLL).await;
            }
            if let (Peers::Hub(session), true) = (&mut self.peers, hub)
                && let Err(error) = session.drained(deadline).await
            {
                eprintln!(
                    "{}: could not announce that it drained, so the cell's other instances wait for it to leave: {error}",
                    self.segment
                );
            }
        }
        #[cfg(not(feature = "hub"))]
        let _ = (segment, deadline, hub);
    }

    /// Resolves when the bound of the stop that ends at `deadline` has
    /// passed. An interior instance whose stop its cell's drain began
    /// (`by_cell`) counts its bound from when every entry instance of the
    /// cell has drained or left, as it did before such a stop could come
    /// earlier, so that a response the cell took before its drain is not
    /// cut sooner than it was. If its lease is lost first, it counts its
    /// bound from the loss, as a stop by a lost lease does.
    /// What it reads is taken now, so that it borrows nothing of the
    /// launch.
    pub(super) fn cell_cutoff(
        &self,
        deadline: tokio::time::Instant,
        by_cell: bool,
    ) -> impl Future<Output = ()> + Send + use<> {
        #[cfg(feature = "hub")]
        let deferred = match (&self.cell_drain, &self.peers) {
            (Some(cell), Peers::Hub(session)) if by_cell && cell.interior() => {
                Some((session.asker(), cell.entry_segments().to_vec()))
            }
            _ => None,
        };
        #[cfg(not(feature = "hub"))]
        let _ = by_cell;
        let bound = self.drain;
        async move {
            #[cfg(feature = "hub")]
            if let Some((asker, entries)) = deferred {
                let entries: Vec<&str> = entries.iter().map(String::as_str).collect();
                let poll = crate::dist::HubSession::poll();
                loop {
                    if asker.is_lost() {
                        break;
                    }
                    let asked = tokio::time::timeout(poll * 10, asker.all_drained(&entries));
                    if matches!(asked.await, Ok(Ok(true))) {
                        break;
                    }
                    tokio::time::sleep(poll).await;
                }
                tokio::time::sleep(bound).await;
                return;
            }
            #[cfg(not(feature = "hub"))]
            let _ = bound;
            tokio::time::sleep_until(deadline).await;
        }
    }

    /// Whether this instance, a source's, says `drained` as a source does,
    /// once its serving body ends. An entry instance of a cell does not:
    /// every instance of a draining cell reads an entry's `drained` as
    /// "this entry holds no request", and a cell drain that begins later
    /// would close an interior instance that a request it holds still
    /// needs. It says it at the end of its drain instead
    /// ([`Launch::cell_entry_drained`]).
    #[cfg(feature = "hub")]
    pub(super) fn source_drains_early(&self) -> bool {
        self.source && self.cell_drain.as_ref().is_none_or(|cell| cell.interior())
    }

    /// For an entry instance of a cell that hosts a source and was stopped
    /// outside its cell's drain: announce that it has drained, retried
    /// until `deadline`, once it holds no request. One that still holds a
    /// request at its bound, or whose lease is lost (`hub` false), says
    /// nothing; its leaving says as much.
    pub(super) async fn cell_entry_drained(
        &mut self,
        segment: &dyn Drains,
        deadline: tokio::time::Instant,
        hub: bool,
    ) {
        #[cfg(feature = "hub")]
        if self.source
            && !self.source_drains_early()
            && hub
            && segment.admitted() == 0
            && let Peers::Hub(session) = &mut self.peers
            && let Err(error) = session.drained(deadline).await
        {
            eprintln!(
                "{}: could not announce that it drained, so the cell's other instances wait for it to leave: {error}",
                self.segment
            );
        }
        #[cfg(not(feature = "hub"))]
        let _ = (segment, deadline, hub);
    }

    /// For an instance of a draining cell that takes no more work, from
    /// outside the cell or, with `closed` false, from inside it: announce
    /// that it detached once no request it holds can send work into the
    /// cell, tried once, for at most ten hub polls. An instance not in
    /// a draining cell, one that does not detach, or one whose lease is
    /// lost (`hub` false) announces nothing. An entry instance that only
    /// declines offers asks its hub first whether the segments of its
    /// class that send to it have detached, so that no hand-off inside the
    /// cell adds a request after it read none; one that `closed` takes no
    /// hand-off at all. Every interior instance waits for its senders to
    /// detach before it drains, or for the entry instances to drain.
    pub(super) async fn cell_detaches(&mut self, segment: &dyn Drains, hub: bool, closed: bool) {
        #[cfg(feature = "hub")]
        if let (Some(cell), Peers::Hub(session), true) = (&self.cell_drain, &mut self.peers, hub)
            && cell.detaches()
            && !self.detached
        {
            // The hub is asked only once the count has fallen to none, and
            // the count is read again after its answer, which may let a
            // hand-off in first.
            if !closed {
                if segment.senders() > 0 {
                    return;
                }
                let asked = tokio::time::timeout(
                    crate::dist::HubSession::poll() * 10,
                    cell.senders_detached(session),
                );
                if !matches!(asked.await, Ok(Ok(true))) {
                    return;
                }
            }
            // Read after the instance stopped taking the work that adds to
            // it, so it only falls from here.
            if segment.senders() > 0 {
                return;
            }
            // Tried once, briefly: the instance's `drained` and its leaving
            // say as much, so a hub that does not take it must not hold the
            // drain that waits here.
            self.detached = true;
            let bound = tokio::time::Instant::now() + crate::dist::HubSession::poll() * 10;
            let announced = async {
                if cell.stalls() {
                    std::future::pending::<()>().await;
                }
                session.detached(bound).await
            };
            let failed = match tokio::time::timeout_at(bound, announced).await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("the hub did not answer in time".to_string()),
            };
            if let Some(error) = failed {
                eprintln!(
                    "{}: could not announce that it detached, so the cell's other instances wait for it to drain or leave: {error}",
                    self.segment
                );
            }
        }
        #[cfg(not(feature = "hub"))]
        let _ = (segment, hub, closed);
    }
}
