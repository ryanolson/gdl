//! A deployment that names its own instantiations of another crate's
//! generic carrier and node, with a builder of its own, and names one of
//! that crate's instantiations again, spelled another way.

use carrier::Carried as Carrier;
use serde::{Deserialize, Serialize};

/// A builder of this deployment's.
#[derive(Serialize, Deserialize, Default)]
pub struct Custom;

/// The carrier, with this deployment's builder.
#[gdl::export]
pub type CustomCarried = Carrier<protocols::Words, Custom>;

/// The relay, with this deployment's builder: its ports carry
/// `CustomCarried`, by type.
#[gdl::export]
pub type CustomRelay = carrier::Relay<Custom>;

/// `read_generic::PlainCarried`, spelled through renames: the same message.
#[gdl::export]
pub type Again = Carrier<protocols::Words, carrier::Plain>;
