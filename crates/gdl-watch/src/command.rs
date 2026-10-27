//! The routes of an operator's commands, under the `gdl-cell` extension's
//! routes, on a cell hub's control listener: revoking one of the cell's
//! models, on the cell's own hub, and withdrawing or scaling to 0 a child
//! cell whose hub is gone, on its parent's hub.

/// The revoke route: `POST` with a [`gdl_model::ModelKey`] revokes the
/// model on the cell. The model leaves the cell's record at once, whatever
/// its instances say, and stays out until the revocation is lifted
/// ([`LIFT_ROUTE`]), also across a restart of the hub. Answered
/// `204 No Content` once the directory has stored a record that leaves the
/// model out, `400 Bad Request` for a malformed model name, and
/// `503 Service Unavailable` when no such record is stored within 5 s, or
/// a later request lifted the revocation first.
pub const REVOKE_ROUTE: &str = "/models/revoke";

/// The lift route: `POST` with a [`gdl_model::ModelKey`] lifts the
/// model's revocation, and the cell's record lists the model again as its
/// instances say, starting afresh. Answered as [`REVOKE_ROUTE`] is, once a
/// record that no longer revokes the model is stored.
pub const LIFT_ROUTE: &str = "/models/lift";

/// The route by which an operator withdraws a child cell whose hub is
/// gone, on the parent's hub: `POST`, with no body. The parent writes the
/// child's withdrawal at an epoch past its last record
/// ([`gdl_cells::CellCommand::Withdraw`]), so no late record of the old
/// epoch brings the cell back. Answered `204 No Content` once the
/// directory has stored it, `404 Not Found` when the parent's directory
/// holds no record of a child of that name and class,
/// `409 Conflict` when the directory refused it, and
/// `503 Service Unavailable` when the directory could not be read or
/// written.
pub const CHILD_WITHDRAW_ROUTE: &str = "/children/{class}/{cell}/withdraw";

/// The route by which an operator says that a child cell whose hub is
/// gone was scaled to 0 on purpose, on the parent's hub: `POST`, with no
/// body. The parent writes the child's last record again, scaled to 0,
/// at an epoch past it ([`gdl_cells::CellCommand::ScaleToZero`]).
/// Answered as [`CHILD_WITHDRAW_ROUTE`] is.
pub const CHILD_SCALE_TO_ZERO_ROUTE: &str = "/children/{class}/{cell}/scale-to-zero";

/// [`CHILD_WITHDRAW_ROUTE`]'s path for the child `cell` of `class`.
pub fn child_withdraw_path(class: &str, cell: &str) -> String {
    format!("/children/{class}/{cell}/withdraw")
}

/// [`CHILD_SCALE_TO_ZERO_ROUTE`]'s path for the child `cell` of `class`.
pub fn child_scale_to_zero_path(class: &str, cell: &str) -> String {
    format!("/children/{class}/{cell}/scale-to-zero")
}
