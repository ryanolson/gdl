//! Pool policies of each kind of head, and impls the reader leaves out.

use gdl::{Pool, PoolPolicy, PoolRequest, Encoded, Projected};
use read_protocols::Words;

/// Reads the head whole.
pub struct Whole;

impl PoolPolicy for Whole {
    type Head = Words;

    fn choose(&self, _: PoolRequest<'_, Words>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
        Err(gdl::NoPool::new())
    }
}

/// Reads the head's bytes.
pub struct Bytes;

impl PoolPolicy for Bytes {
    type Head = Encoded;

    fn choose(&self, _: PoolRequest<'_, Encoded>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
        Err(gdl::NoPool::new())
    }
}

/// The part of `Words` a policy reads.
#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Words)]
pub struct Part {}

/// Reads no head: the request is `()`.
pub struct Unit;

impl PoolPolicy for Unit {
    type Head = ();

    fn choose(&self, _: PoolRequest<'_, ()>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
        Err(gdl::NoPool::new())
    }
}

pub mod globbed {
    use gdl::*;

    /// Names the trait through a glob of gdl.
    pub struct Globbed;

    impl PoolPolicy for Globbed {
        type Head = Encoded;

        fn choose(&self, _: PoolRequest<'_, Encoded>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::new())
        }
    }
}

pub use globbed::Globbed;

pub mod local {
    /// A trait of gdl's name that is not gdl's.
    pub trait PoolPolicy {
        type Head;
    }

    /// Implements only the local trait.
    pub struct Imposter;

    impl PoolPolicy for Imposter {
        type Head = gdl::Encoded;
    }
}

pub use local::Imposter;

pub mod nested {
    use super::*;

    /// Reads a part of the head; reached from the root by a `use`.
    pub struct Parted;

    impl PoolPolicy for Parted {
        type Head = Projected<Part>;

        fn choose(&self, _: PoolRequest<'_, Projected<Part>>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::new())
        }
    }
}

pub use nested::Parted;

mod hidden {
    use super::*;

    /// Not reached from the crate root, so no segment could name it.
    pub struct Hidden;

    impl PoolPolicy for Hidden {
        type Head = Encoded;

        fn choose(&self, _: PoolRequest<'_, Encoded>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::new())
        }
    }
}

/// Generic: no one type to name.
pub struct Generic<T>(T);

impl<T: Send + Sync + 'static> PoolPolicy for Generic<T> {
    type Head = Encoded;

    fn choose(&self, _: PoolRequest<'_, Encoded>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
        Err(gdl::NoPool::new())
    }
}

pub mod parent {
    use super::{Pool, PoolPolicy, PoolRequest};

    /// Names the trait through its parent's `use` of it.
    pub struct FromParent;

    impl PoolPolicy for FromParent {
        type Head = ();

        fn choose(&self, _: PoolRequest<'_, ()>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::new())
        }
    }
}

pub use parent::FromParent;

pub mod shadow {
    /// A module of the crate's own that is named `gdl`.
    pub mod gdl {
        pub trait PoolPolicy {
            type Head;
        }
    }

    use gdl::*;

    /// Implements the trait of the local `gdl` module, not gdl's.
    pub struct Shadowed;

    impl PoolPolicy for Shadowed {
        type Head = ();
    }
}

pub use shadow::Shadowed;

pub mod aliased {
    use gdl::PoolPolicy as Routing;

    pub mod inner {
        use super::Routing;

        /// Names the trait through its parent's rename of it.
        pub struct Renamed;

        impl Routing for Renamed {
            type Head = gdl::Encoded;

            fn choose(&self, _: gdl::PoolRequest<'_, gdl::Encoded>, _: &[gdl::Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
                Err(gdl::NoPool::new())
            }
        }
    }
}

pub use aliased::inner::Renamed;

pub mod crate_import {
    use gdl::{self, Pool, PoolRequest};

    /// Names the trait through an explicit import of the gdl crate.
    pub struct ThroughCrate;

    impl gdl::PoolPolicy for ThroughCrate {
        type Head = ();

        fn choose(&self, _: PoolRequest<'_, ()>, _: &[Pool]) -> Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::new())
        }
    }
}

pub use crate_import::ThroughCrate;

pub mod globbed_facade {
    pub mod facade {
        pub mod gdl {
            pub trait PoolPolicy {
                type Head;
            }
        }
    }

    use ::gdl::*;
    use facade::*;

    /// Implements the trait of the facade's `gdl`, which its glob brings
    /// after a glob of gdl itself.
    pub struct Faked;

    impl gdl::PoolPolicy for Faked {
        type Head = ();
    }
}

pub use globbed_facade::Faked;
