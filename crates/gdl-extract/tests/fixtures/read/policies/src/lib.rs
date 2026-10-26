//! Cell policies of each kind of head, and impls the reader leaves out.

use gdl::{Cell, CellPolicy, CellRequest, Encoded, Projected};
use read_protocols::Words;

/// Reads the head whole.
pub struct Whole;

impl CellPolicy for Whole {
    type Head = Words;

    fn choose(&self, _: CellRequest<'_, Words>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
        Err(gdl::NoCell::new())
    }
}

/// Reads the head's bytes.
pub struct Bytes;

impl CellPolicy for Bytes {
    type Head = Encoded;

    fn choose(&self, _: CellRequest<'_, Encoded>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
        Err(gdl::NoCell::new())
    }
}

/// The part of `Words` a policy reads.
#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Words)]
pub struct Part {}

/// Reads no head: the request is `()`.
pub struct Unit;

impl CellPolicy for Unit {
    type Head = ();

    fn choose(&self, _: CellRequest<'_, ()>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
        Err(gdl::NoCell::new())
    }
}

pub mod globbed {
    use gdl::*;

    /// Names the trait through a glob of gdl.
    pub struct Globbed;

    impl CellPolicy for Globbed {
        type Head = Encoded;

        fn choose(&self, _: CellRequest<'_, Encoded>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
            Err(gdl::NoCell::new())
        }
    }
}

pub use globbed::Globbed;

pub mod local {
    /// A trait of gdl's name that is not gdl's.
    pub trait CellPolicy {
        type Head;
    }

    /// Implements only the local trait.
    pub struct Imposter;

    impl CellPolicy for Imposter {
        type Head = gdl::Encoded;
    }
}

pub use local::Imposter;

pub mod nested {
    use super::*;

    /// Reads a part of the head; reached from the root by a `use`.
    pub struct Parted;

    impl CellPolicy for Parted {
        type Head = Projected<Part>;

        fn choose(&self, _: CellRequest<'_, Projected<Part>>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
            Err(gdl::NoCell::new())
        }
    }
}

pub use nested::Parted;

mod hidden {
    use super::*;

    /// Not reached from the crate root, so no segment could name it.
    pub struct Hidden;

    impl CellPolicy for Hidden {
        type Head = Encoded;

        fn choose(&self, _: CellRequest<'_, Encoded>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
            Err(gdl::NoCell::new())
        }
    }
}

/// Generic: no one type to name.
pub struct Generic<T>(T);

impl<T: Send + Sync + 'static> CellPolicy for Generic<T> {
    type Head = Encoded;

    fn choose(&self, _: CellRequest<'_, Encoded>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
        Err(gdl::NoCell::new())
    }
}

pub mod parent {
    use super::{Cell, CellPolicy, CellRequest};

    /// Names the trait through its parent's `use` of it.
    pub struct FromParent;

    impl CellPolicy for FromParent {
        type Head = ();

        fn choose(&self, _: CellRequest<'_, ()>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
            Err(gdl::NoCell::new())
        }
    }
}

pub use parent::FromParent;

pub mod shadow {
    /// A module of the crate's own that is named `gdl`.
    pub mod gdl {
        pub trait CellPolicy {
            type Head;
        }
    }

    use gdl::*;

    /// Implements the trait of the local `gdl` module, not gdl's.
    pub struct Shadowed;

    impl CellPolicy for Shadowed {
        type Head = ();
    }
}

pub use shadow::Shadowed;

pub mod aliased {
    use gdl::CellPolicy as Routing;

    pub mod inner {
        use super::Routing;

        /// Names the trait through its parent's rename of it.
        pub struct Renamed;

        impl Routing for Renamed {
            type Head = gdl::Encoded;

            fn choose(&self, _: gdl::CellRequest<'_, gdl::Encoded>, _: &[gdl::Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
                Err(gdl::NoCell::new())
            }
        }
    }
}

pub use aliased::inner::Renamed;

pub mod crate_import {
    use gdl::{self, Cell, CellRequest};

    /// Names the trait through an explicit import of the gdl crate.
    pub struct ThroughCrate;

    impl gdl::CellPolicy for ThroughCrate {
        type Head = ();

        fn choose(&self, _: CellRequest<'_, ()>, _: &[Cell]) -> Result<gdl::CellChoice, gdl::NoCell> {
            Err(gdl::NoCell::new())
        }
    }
}

pub use crate_import::ThroughCrate;

pub mod globbed_facade {
    pub mod facade {
        pub mod gdl {
            pub trait CellPolicy {
                type Head;
            }
        }
    }

    use ::gdl::*;
    use facade::*;

    /// Implements the trait of the facade's `gdl`, which its glob brings
    /// after a glob of gdl itself.
    pub struct Faked;

    impl gdl::CellPolicy for Faked {
        type Head = ();
    }
}

pub use globbed_facade::Faked;
