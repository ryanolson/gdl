//! The routes of an operator's commands, under the `gdl-pool` extension's
//! routes, on a pool hub's control listener: revoking one of the pool's
//! properties, on the pool's own hub, and withdrawing or scaling to 0 a
//! child pool whose hub is gone, on its parent's hub.

/// The revoke route: `POST` with a [`gdl_model::PropertyKey`], `{ "kind":
/// ..., "key": { ... } }`, revokes the property on the pool. The property
/// leaves the pool's record at once, whatever its instances say, and stays
/// out until the revocation is lifted ([`LIFT_ROUTE`]), also across a
/// restart of the hub. The key is compared in its canonical form, so its
/// fields can be written in any order. Answered `204 No Content` once the
/// directory has stored a record that leaves the property out, `400 Bad
/// Request` for a malformed property, and `503 Service Unavailable` when
/// no such record is stored within 5 s, or a later request lifted the
/// revocation first.
pub const REVOKE_ROUTE: &str = "/properties/revoke";

/// The lift route: `POST` with a [`gdl_model::PropertyKey`] lifts the
/// property's revocation, and the pool's record lists the property again
/// as its instances say, starting afresh. Answered as [`REVOKE_ROUTE`] is,
/// once a record that no longer revokes the property is stored.
pub const LIFT_ROUTE: &str = "/properties/lift";

/// The route by which an operator withdraws a child pool whose hub is
/// gone, on the parent's hub: `POST`, with no body. The parent writes the
/// child's withdrawal at an epoch past its last record
/// ([`gdl_pools::PoolCommand::Withdraw`]), so no late record of the old
/// epoch brings the pool back. Answered `204 No Content` once the
/// directory has stored it, `404 Not Found` when the parent's directory
/// holds no record of a child of that name and class,
/// `409 Conflict` when the directory refused it, and
/// `503 Service Unavailable` when the directory could not be read or
/// written.
pub const CHILD_WITHDRAW_ROUTE: &str = "/children/{class}/{pool}/withdraw";

/// The route by which an operator says that a child pool whose hub is
/// gone was scaled to 0 on purpose, on the parent's hub: `POST`, with no
/// body. The parent writes the child's last record again, scaled to 0,
/// at an epoch past it ([`gdl_pools::PoolCommand::ScaleToZero`]).
/// Answered as [`CHILD_WITHDRAW_ROUTE`] is.
pub const CHILD_SCALE_TO_ZERO_ROUTE: &str = "/children/{class}/{pool}/scale-to-zero";

/// [`CHILD_WITHDRAW_ROUTE`]'s path for the child `pool` of `class`.
pub fn child_withdraw_path(class: &str, pool: &str) -> String {
    format!("/children/{class}/{pool}/withdraw")
}

/// [`CHILD_SCALE_TO_ZERO_ROUTE`]'s path for the child `pool` of `class`.
pub fn child_scale_to_zero_path(class: &str, pool: &str) -> String {
    format!("/children/{class}/{pool}/scale-to-zero")
}
