//! A relay scripted by a test: its watches, and its watches of one cell.

use std::collections::VecDeque;
use std::sync::Mutex;

use futures::StreamExt;
use futures::stream::BoxStream;
use gdl_watch::{FollowEvent, Watched};

use super::super::task::{Relayed, Source};

/// Watches scripted in order, each a list of items that ends the watch,
/// or, for a relaying script, stays open after them; after the last, a
/// watch that never ends. Each watch asked for is
/// counted, and begins only once the test has released that many. Each
/// watch of one cell asked for is recorded, and answered from `readings`
/// in order, each open after its items, once the test has released that
/// many.
pub(super) struct Scripted {
    watches: Mutex<VecDeque<Vec<Relayed>>>,
    opened: tokio::sync::watch::Sender<usize>,
    released: tokio::sync::watch::Receiver<usize>,
    pub(super) readings: Mutex<VecDeque<Vec<Relayed>>>,
    pub(super) asked: tokio::sync::watch::Sender<Vec<(String, String)>>,
    pub(super) answered: tokio::sync::watch::Receiver<usize>,
    /// Whether a watch stays open after its items.
    open: bool,
}

impl Scripted {
    /// `watches` of known events, and no reading.
    pub(super) fn new(
        watches: VecDeque<Vec<FollowEvent>>,
        opened: tokio::sync::watch::Sender<usize>,
        released: tokio::sync::watch::Receiver<usize>,
    ) -> Self {
        let watches = watches
            .into_iter()
            .map(|events| events.into_iter().map(Watched::Event).collect())
            .collect();
        Self {
            open: false,
            ..Self::relaying(watches, opened, released)
        }
    }

    /// `watches`, each open after its items, and no reading.
    pub(super) fn relaying(
        watches: VecDeque<Vec<Relayed>>,
        opened: tokio::sync::watch::Sender<usize>,
        released: tokio::sync::watch::Receiver<usize>,
    ) -> Self {
        Self {
            watches: Mutex::new(watches),
            opened,
            released,
            readings: Mutex::default(),
            asked: tokio::sync::watch::Sender::new(Vec::new()),
            answered: tokio::sync::watch::channel(0).1,
            open: true,
        }
    }
}

impl Source for Scripted {
    async fn watch_of(
        &self,
        class: &str,
        cell: &str,
    ) -> Result<BoxStream<'static, Relayed>, String> {
        let mut asked = 0;
        self.asked.send_modify(|cells| {
            cells.push((class.to_string(), cell.to_string()));
            asked = cells.len();
        });
        let mut answered = self.answered.clone();
        answered
            .wait_for(|answered| *answered >= asked)
            .await
            .map_err(|_| "the test ended".to_string())?;
        let items = self
            .readings
            .lock()
            .expect("lock")
            .pop_front()
            .ok_or_else(|| "no reading scripted".to_string())?;
        Ok(futures::stream::iter(items)
            .chain(futures::stream::pending())
            .boxed())
    }

    async fn watch(&self) -> Result<BoxStream<'static, Relayed>, String> {
        let mut opened = 0;
        self.opened.send_modify(|count| {
            *count += 1;
            opened = *count;
        });
        let mut released = self.released.clone();
        released
            .wait_for(|released| *released >= opened)
            .await
            .map_err(|_| "the test ended".to_string())?;
        let next = self.watches.lock().expect("lock").pop_front();
        Ok(match next {
            Some(events) if self.open => futures::stream::iter(events)
                .chain(futures::stream::pending())
                .boxed(),
            Some(events) => futures::stream::iter(events).boxed(),
            None => futures::stream::pending().boxed(),
        })
    }

    async fn declare(&self) -> Result<(), String> {
        Ok(())
    }

    fn acknowledge(
        &self,
        _: gdl_watch::Unoffered,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn avoid(
        &self,
        _: gdl_watch::Avoids,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
