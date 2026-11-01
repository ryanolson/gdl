//! The rule of one pool's damping as a budget's decline sees it: a pure
//! state machine, so that the rule is checked whole, over every order of
//! its events ([`tests`]), apart from the lists and the clock the chooser
//! keeps it in ([`super::Damping`]).
//!
//! The rule:
//!
//! - a budget's decline of the summary `v` passes the pool over until one
//!   of its periods has passed, unless the chooser's table already shows a
//!   newer summary of the pool, which is a new budget;
//! - a mark of an older summary gives way to it;
//! - a mark of a newer summary stays;
//! - a mark of the same summary stays while it lasts, and is renewed once
//!   it has passed, since the pool still declines while its next summary
//!   lags;
//! - a full pool's window that still lasts is left as it is;
//! - a full pool's decline leaves any mark that still lasts, and otherwise
//!   starts a window;
//! - an acceptance ends a full pool's window, and leaves a budget's mark: a
//!   pinned turn's acceptance says nothing of the budget;
//! - a newer summary of the pool ends a budget's mark.

/// What one pool's damping holds, at instants of type `T`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held<T> {
    /// Nothing.
    Clear,
    /// A full pool's window, until `until`.
    Window { until: T },
    /// A budget's mark of the summary `view`, (epoch, sequence), until
    /// `until`.
    Budget { view: (u64, u64), until: T },
}

impl<T: Copy + Ord> Held<T> {
    /// Whether what is held still lasts at `now`.
    pub(crate) fn lasts(&self, now: T) -> bool {
        match self {
            Self::Clear => false,
            Self::Window { until } | Self::Budget { until, .. } => *until > now,
        }
    }

    /// The pool declined an offer of the summary `view` over its budget at
    /// `now`, while the chooser's table shows the summary `shown` of it:
    /// what is held after, a mark until `until` if one is set.
    pub(crate) fn budget(
        self,
        view: (u64, u64),
        shown: Option<(u64, u64)>,
        now: T,
        until: T,
    ) -> Self {
        if shown.is_some_and(|shown| shown > view) {
            return self;
        }
        let mark = Self::Budget { view, until };
        match self {
            Self::Budget { view: held, .. } if held < view => mark,
            Self::Budget { view: held, .. } if held > view => self,
            Self::Budget { .. } | Self::Window { .. } if self.lasts(now) => self,
            Self::Budget { .. } | Self::Window { .. } | Self::Clear => mark,
        }
    }

    /// The pool declined an offer as full at `now`: a window until `until`,
    /// unless what is held still lasts.
    pub(crate) fn full(self, now: T, until: T) -> Self {
        if self.lasts(now) {
            self
        } else {
            Self::Window { until }
        }
    }

    /// The chooser's table shows the summary `shown` of the pool.
    pub(crate) fn summary(self, shown: (u64, u64)) -> Self {
        match self {
            Self::Budget { view, .. } if view < shown => Self::Clear,
            _ => self,
        }
    }

    /// The pool accepted an offer.
    pub(crate) fn accept(self) -> Self {
        match self {
            Self::Window { .. } => Self::Clear,
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Held;

    /// One event of a pool's damping, at whole instants.
    #[derive(Debug, Clone, Copy)]
    enum Event {
        /// A budget's decline of the summary (1, n).
        Budget(u64),
        /// A full pool's decline.
        Full,
        /// The table shows the summary (1, n).
        Summary(u64),
        /// An acceptance.
        Accept,
        /// One instant passes.
        Tick,
    }

    /// A budget's mark lasts this long.
    const PERIOD: u32 = 2;
    /// A full pool's window lasts this long.
    const WINDOW: u32 = 1;

    /// The rule as the pull request states it, kept apart from
    /// [`Held`]: a budget's mark and a full pool's window are two
    /// variables, and each sentence of the rule is one condition.
    #[derive(Debug, Default)]
    struct Reference {
        now: u32,
        shown: Option<(u64, u64)>,
        mark: Option<((u64, u64), u32)>,
        window: Option<u32>,
    }

    impl Reference {
        fn mark_lasts(&self) -> bool {
            self.mark.is_some_and(|(_, until)| until > self.now)
        }

        fn window_lasts(&self) -> bool {
            self.window.is_some_and(|until| until > self.now)
        }

        fn apply(&mut self, event: Event) {
            match event {
                Event::Budget(n) => {
                    let view = (1, n);
                    // A newer summary shown is a new budget: nothing.
                    if self.shown.is_some_and(|shown| shown > view) {
                        return;
                    }
                    let set = match self.mark {
                        // An older summary's mark gives way, lasting or not.
                        Some((held, _)) if held < view => true,
                        // A newer summary's mark stays.
                        Some((held, _)) if held > view => false,
                        // The same summary's: kept while it lasts, renewed after.
                        Some(_) if self.mark_lasts() => false,
                        // A full pool's lasting window is left as it is.
                        _ if self.window_lasts() => false,
                        _ => true,
                    };
                    if set {
                        self.mark = Some((view, self.now + PERIOD));
                        self.window = None;
                    }
                }
                Event::Full => {
                    if !self.mark_lasts() && !self.window_lasts() {
                        self.window = Some(self.now + WINDOW);
                        self.mark = None;
                    }
                }
                Event::Summary(n) => {
                    let shown = (1, n);
                    self.shown = Some(self.shown.map_or(shown, |held| held.max(shown)));
                    if self.mark.is_some_and(|(view, _)| view < shown) {
                        self.mark = None;
                    }
                }
                Event::Accept => self.window = None,
                Event::Tick => self.now += 1,
            }
        }

        /// What passes the pool over now: the instant it ends, and the
        /// summary of a budget's mark.
        fn passed_over(&self) -> Option<(u32, Option<(u64, u64)>)> {
            if let Some((view, until)) = self.mark.filter(|_| self.mark_lasts()) {
                return Some((until, Some(view)));
            }
            self.window
                .filter(|_| self.window_lasts())
                .map(|until| (until, None))
        }
    }

    /// [`Held`] as the chooser drives it: the table's summary is the
    /// latest it was shown.
    #[derive(Debug)]
    struct Machine {
        now: u32,
        shown: Option<(u64, u64)>,
        held: Held<u32>,
    }

    impl Machine {
        fn apply(&mut self, event: Event) {
            let now = self.now;
            self.held = match event {
                Event::Budget(n) => self.held.budget((1, n), self.shown, now, now + PERIOD),
                Event::Full => self.held.full(now, now + WINDOW),
                Event::Summary(n) => {
                    let shown = (1, n);
                    self.shown = Some(self.shown.map_or(shown, |held| held.max(shown)));
                    self.held.summary(shown)
                }
                Event::Accept => self.held.accept(),
                Event::Tick => {
                    self.now += 1;
                    self.held
                }
            };
        }

        fn passed_over(&self) -> Option<(u32, Option<(u64, u64)>)> {
            if !self.held.lasts(self.now) {
                return None;
            }
            match self.held {
                Held::Clear => None,
                Held::Window { until } => Some((until, None)),
                Held::Budget { view, until } => Some((until, Some(view))),
            }
        }
    }

    /// Every order of up to five events over three summaries leaves the
    /// machine passing the pool over exactly as the stated rule does, after
    /// each event: until the same instant, for the same summary.
    #[test]
    fn the_machine_keeps_the_rule_over_every_order_of_its_events() {
        let events = [
            Event::Budget(1),
            Event::Budget(2),
            Event::Budget(3),
            Event::Summary(1),
            Event::Summary(2),
            Event::Summary(3),
            Event::Full,
            Event::Accept,
            Event::Tick,
        ];
        let mut checked = 0;
        for length in 1..=5u32 {
            for index in 0..events.len().pow(length) {
                let mut reference = Reference::default();
                let mut machine = Machine {
                    now: 0,
                    shown: None,
                    held: Held::Clear,
                };
                let mut at = index;
                let mut trace = Vec::new();
                for _ in 0..length {
                    let event = events[at % events.len()];
                    at /= events.len();
                    trace.push(event);
                    reference.apply(event);
                    machine.apply(event);
                    assert_eq!(
                        machine.passed_over(),
                        reference.passed_over(),
                        "after {trace:?}"
                    );
                }
                checked += 1;
            }
        }
        assert_eq!(checked, 9 + 81 + 729 + 6_561 + 59_049);
    }
}
