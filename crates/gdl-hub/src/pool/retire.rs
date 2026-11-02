//! The retire requests the pool's hub holds ([`gdl_watch::RETIRE_ROUTE`]):
//! each member of the pool's class, entry or interior, by its current
//! registration, and whether a controller asked it to retire in that
//! registration. The feed keeps it under its own lock, so that a request
//! is sent on the watch, and shown in a snapshot, in order with the rest;
//! and a request goes with the registration it names, so that one taken
//! just before the registration ended is never shown after it.

use std::collections::HashMap;

use gdl_watch::RetireRequest;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::InstanceId;

/// The members, by their current registration.
#[derive(Default)]
pub(super) struct Requests {
    members: HashMap<InstanceId, Member>,
}

struct Member {
    incarnation: Incarnation,
    requested: bool,
}

/// Why a request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refused {
    /// The instance is no member of the pool's class.
    Unknown,
    /// The incarnation is not the member's current one.
    Stale,
    /// The pool drains, which already stops every instance.
    Draining,
}

impl Requests {
    /// `id` registered in `incarnation`, as a `member` of the pool's
    /// class or not: a request for an earlier registration of it ended
    /// with that registration, and one outside the class is no member.
    pub(super) fn joined(&mut self, id: InstanceId, incarnation: Incarnation, member: bool) {
        match self.members.get_mut(&id) {
            Some(held) if held.incarnation >= incarnation => {}
            Some(_) if !member => {
                self.members.remove(&id);
            }
            None if !member => {}
            Some(held) => {
                held.incarnation = incarnation;
                held.requested = false;
            }
            None => {
                self.members.insert(
                    id,
                    Member {
                        incarnation,
                        requested: false,
                    },
                );
            }
        }
    }

    /// The hub removed a registration: its member, and its request, go
    /// with it, unless a later registration of it has been committed.
    pub(super) fn removed(&mut self, removal: &Removal) {
        if self
            .members
            .get(&removal.id)
            .is_some_and(|member| member.incarnation <= removal.incarnation)
        {
            self.members.remove(&removal.id);
        }
    }

    /// Ask `id`, in its registration `incarnation`, to retire, unless the
    /// pool `draining`: the request to send, when it is new.
    pub(super) fn request(
        &mut self,
        id: InstanceId,
        incarnation: u64,
        draining: bool,
    ) -> Result<Option<RetireRequest>, Refused> {
        let member = self.members.get_mut(&id).ok_or(Refused::Unknown)?;
        if draining {
            return Err(Refused::Draining);
        }
        if member.incarnation.as_u64() != incarnation {
            return Err(Refused::Stale);
        }
        if std::mem::replace(&mut member.requested, true) {
            return Ok(None);
        }
        Ok(Some(RetireRequest {
            instance: id,
            incarnation,
        }))
    }

    /// Every request held, in the order of the members' ids.
    pub(super) fn held(&self) -> Vec<RetireRequest> {
        let mut held: Vec<RetireRequest> = self
            .members
            .iter()
            .filter(|(_, member)| member.requested)
            .map(|(id, member)| RetireRequest {
                instance: *id,
                incarnation: member.incarnation.as_u64(),
            })
            .collect();
        held.sort_by_key(|request| request.instance.as_u128());
        held
    }
}

#[cfg(test)]
mod tests {
    use super::{Refused, Requests};
    use gdl_watch::RetireRequest;
    use velo_hub::store::{Incarnation, Removal};
    use velo_hub::velo::InstanceId;

    fn removal(id: InstanceId, incarnation: u64) -> Removal {
        Removal {
            id,
            incarnation: Incarnation::from_u64(incarnation),
        }
    }

    /// A removal of an earlier registration, delivered after a later one
    /// was committed, leaves the member and its request.
    #[test]
    fn a_late_removal_of_an_earlier_registration_changes_nothing() {
        let mut requests = Requests::default();
        let id = InstanceId::new_v4();
        requests.joined(id, Incarnation::from_u64(3), true);
        requests.joined(id, Incarnation::from_u64(5), true);
        assert_eq!(requests.request(id, 3, false), Err(Refused::Stale));
        assert!(requests.request(id, 5, false).expect("taken").is_some());
        let held = vec![RetireRequest {
            instance: id,
            incarnation: 5,
        }];
        requests.removed(&removal(id, 3));
        assert_eq!(requests.held(), held);
        // A commit of the earlier one, delivered late, keeps the later.
        requests.joined(id, Incarnation::from_u64(3), true);
        assert_eq!(requests.held(), held);
        requests.removed(&removal(id, 5));
        assert_eq!(requests.request(id, 5, false), Err(Refused::Unknown));
        assert!(requests.held().is_empty());
    }

    /// An unknown instance is unknown whether or not the pool drains; a
    /// member of a draining pool is refused before its incarnation is
    /// read.
    #[test]
    fn the_refusals_are_told_apart() {
        let mut requests = Requests::default();
        let id = InstanceId::new_v4();
        assert_eq!(requests.request(id, 1, true), Err(Refused::Unknown));
        requests.joined(id, Incarnation::from_u64(1), true);
        assert_eq!(requests.request(id, 2, true), Err(Refused::Draining));
        assert_eq!(requests.request(id, 1, true), Err(Refused::Draining));
        assert!(requests.held().is_empty());
    }
}
